/*
 * database.h
 *
 *  Created on: 5. 2. 2019
 *      Author: ondra
 */

#ifndef SRC_LIBSOFA_DATABASECORE_H_
#define SRC_LIBSOFA_DATABASECORE_H_

#include <map>
#include <vector>
#include <queue>
#include <memory>
#include <imtjson/value.h>
#include "types.h"
#include "kvapi.h"
#include <mutex>

namespace sofadb {

class DatabaseCore {
public:

	typedef std::uint32_t Handle;
	static const Handle invalid_handle = static_cast<Handle>(-1);

	typedef std::function<void()> Callback;

private:
	struct WriteState {
		PChangeset curBatch;
		unsigned int lockCount = 0;
		std::queue<Callback> waiting;
	};

	struct ViewState {
		///reference to script (or script self) to build this view
		std::string scriptRef;
		///sequence number of last update
		SeqNum seqNum = 0;
		///functions waiting to finish update
		std::queue<Callback> waiting;
		///true if the view is being updated
		bool updating = false;
	};

	typedef std::map<ViewID, ViewState> ViewStateMap;

	struct Info {
		std::string name;
		SeqNum nextSeqNum = 1;
		WriteState writeState;
		ViewStateMap viewState;
		///temporary buffer for key
		std::string key,key2;
		///temporary buffer for value
		std::string value,value2;

		std::recursive_mutex lock;
	};

	typedef std::map<std::string_view, Handle, std::less<> > NameToIDMap;
	typedef std::vector<std::unique_ptr<Info> > DBList;

	class PInfo {
	public:
		Info *ptr;
		PInfo(Info *ptr):ptr(ptr) {if (ptr) ptr->lock.lock();}
		PInfo(const PInfo &other):ptr(other.ptr) {if (ptr) ptr->lock.lock();}
		PInfo(PInfo &&other):ptr(other.ptr) {other.ptr = nullptr;}
		~PInfo() {if (ptr) ptr->lock.unlock();}
		Info *operator->() {return ptr;}
		bool operator==(nullptr_t) const {return ptr == nullptr;}
		bool operator!=(nullptr_t) const {return ptr != nullptr;}
	};


public:

	DatabaseCore();
	virtual ~DatabaseCore();

	void open(std::string_view name);



	Handle create(const std::string_view &name);
	Handle getHandle(const std::string_view &name) const;
	void erase(Handle h);


	template<typename Fn> auto list(Fn &&fn) const -> decltype((bool)fn(std::string())) {
		for(auto &&c : idmap) if (!fn(c.first)) return false;
		return true;
	}
	template<typename Fn> auto list(Fn &&fn) const -> decltype((bool)fn(std::string(),Handle())) {
		for(auto &&c : idmap) if (!fn(c.first,c.second)) return false;
		return true;
	}


	struct RawDocument {
		///ID of the document
		DocID docId;
		///Revision of the document (should be hash of data)
		RevID revision;
		///rest of the document is stored as json
		json::Value content;
	};

	struct DocumentInfo {
		std::string_view docid;
		RevID revision;
		SeqNum seq_number;
		json::Value content;
	};

	struct ViewUpdateRow {
		std::string_view key;
		std::string_view value;
	};


	struct ViewResult: public ViewUpdateRow {
		std::string_view docid;
	};



	///Opens batch write for the database
	/** When batch is opened, none of writes are goes directly to the database until the batch is closed.
	 * There can be only single batch per database
	 * @param h database handle
	 * @param exclusive Requests to open batch in exclusive mode.
	 *
	 * @retval true batch opened.
	 * @retval false batch cannot be opened, because batch is already opened or database is no longer exists
	 *
	 * @note if batch is opened by multiple times, it must be closed by calling same count of endBatch.
	 * Only when internal counter of opening is reaches zero, the batch is flushed
	 */


	bool beginBatch(Handle h, bool exclusive = false);
	///Stores update to the DB
	/** Note function provides just raw store, without doing validation. It should be
	 * done elsewhere
	 *
	 * @param h handle to database
	 * @param doc document to write
	 *
	 * Each document is assigned seqID
	 */
	bool storeUpdate(Handle h, const RawDocument &doc);


	///Called after batch is successfuly closed
	/** Allows to open batch exlusivelly
	 *
	 * @param h handle to database
	 * @param cb callback function called when batch is closed
	 * @retval true success
	 * @retval false database no longer exists
	 *
	 * */
	bool onBatchClose(Handle h, Callback &&cb);


	///Closes batch and flushes all changes to DB
	void endBatch(Handle h);


	///Retrieve single document from the database
	/**
	 * @param h handle to database
	 * @param docid ID of document
	 * @param content this structure is filled by content
	 * @param only_header set true, if you need just only header - will not set JSON part of the document
	 * @retval true found
	 * @retval false not found
	 */
	bool findDoc(Handle h, const std::string_view &docid, DocumentInfo &content, bool only_header = false);

	///Retrieve historical document from the database
	/**
	 *
	 * @param h handle to databse
	 * @param docid document id
	 * @param revid revision id
	 * @param content this strutcure is filled by content
	 * @param only_header set true, if you need just only header - in this case, only seqnum is viable
	 * @retval true found
	 * @retval false not found
	 */
	bool findDoc(Handle h, const std::string_view &docid, RevID revid, DocumentInfo &content, bool only_header = false);


	///Lists all revisions of the document
	/**
	 * @param h handle to the database
	 * @param docid document id
	 * @param callback function called for each revision
	 * @param only_header set true, if you need just only header
	 *
	 * @note revisions are not listed in the order. You need to sort them by seq_number
	 */
	bool enumAllRevisions(Handle h, const std::string_view &docid, std::function<void(const DocumentInfo &)> callback, bool only_header = false);


	///Erases doc
	/**
	 * This erases doc from the database. However it is not recomended use
	 * this function, because it can break replication. It is better to mark document
	 * as deleted (aka make tombstone). However it can be sometimes useful to erase
	 * old tombstones
	 *
	 * @param h
	 * @param docid
	 *
	 * @note function can take a time and block the database, because it is need
	 * to remove document from all views
	 */
	void eraseDoc(Handle h, const std::string_view &docid);


	///Erases historical doc
	/**
	 * @param h handle to db
	 * @param docid document id
	 * @param revision revision id
	 *
	 * @note doesn't erase current revision.
	 */
	void eraseHistoricalDoc(Handle h, const std::string_view &docid, RevID revision);


	void enumDocs(Handle h, const std::string_view &prefix,  bool reversed, std::function<void(const DocumentInfo &)> callback, bool header_only = false);
	void enumDocs(Handle h, const std::string_view &start_include, const std::string_view &end_exclude, std::function<void(const DocumentInfo &)> callback, bool header_only = false);

	///Finds document by sequence number if exists
	/**
	 * @param h handle to database
	 * @param seqNum sequence number
	 * @param docid found document ID
	 * @retval true found
	 * @retval false not found
	 */
	bool findDocBySeqNum(Handle h, SeqNum seqNum, DocID &docid);

	///Reads changes
	/**
	 * @param h handle to database
	 * @param from seqnum where reading starts
	 * @param reversed set true to read backward (from present to history)
	 * @param fn function called with every result
	 * @return SeqNum of last result, if zero returned, then no records are found for this DB
	 */
	SeqNum readChanges(Handle h, SeqNum from, bool reversed, std::function<bool(const DocID&, const SeqNum &)> &&fn);


	///Makes lookup to a view for key
	/**
	 * @param viewID ID of view
	 * @param key key to search - function always treat this key as prefix
	 * @param reversed - return records in reversed order
	 * @param callback - function called for every record. Function return false to stop reading
	 * @retval true found results
	 * @retval false nothing found
	 */
	bool viewLookup(ViewID viewID, const std::string_view &key, bool reversed,  std::function<bool(const ViewResult &)> &&callback);

	///Makes lookup to a view for given key range
	/**
	 * @param viewID ID of view
	 * @param start_key first key in lookup
	 * @param end_key end key in lookup - it is always excluded
	 * @param start_doc first document of that key, it can be empty to start from the begining of the key
	 * @param end_doc last document of the end_key, which will be excluded
	 * @param callback function called for every result
	 * @retval true found results
	 * @retval false nothing found
	 */
	bool viewLookup(ViewID viewID,
					const std::string_view &start_key,
					const std::string_view &end_key,
					const std::string_view &start_doc,
					const std::string_view &end_doc,
					std::function<bool(const ViewResult &)> &&callback);

	///Checks whether view need update
	/**
	 * @param h handle to db
	 * @param view view to view
	 * @retval 0 the view doesn't need to update (or not exists)
	 * @retval >0 the view need update from given seq_number.
	 *
	 * @note seqnum always starts on 1, so zero is used as "not need update".
	 */
	SeqNum needViewUpdate(Handle h, ViewID view) const;

	///Should be called after view update is done
	/**
	 * @param h handle to db
	 * @param view view id
	 * @param seqNum sequence number
	 * @retval true updated
	 * @retval false view not found
	 *
	 * @note it is better to open write batch to better performance
	 */
	bool updateViewState(Handle h, ViewID view, SeqNum seqNum);


	///Updates document in the view
	/**
	 *
	 * @param view id of view
	 * @param docId document id
	 * @param updates key-value records for the document. You need to send all keys
	 * in a single update. If this array is empty, the function just deletes the
	 * document from the view
	 *
	 * @note the function first delete previous update
	 */
	void view_updateDocument(ViewID view, const std::string_view &docId,
				const std::basic_string_view<ViewUpdateRow> &updates);

protected:

	PKeyValueDatabase maindb;
	DBList dblist;
	NameToIDMap idmap;
	ViewID nextViewID;


	Handle allocSlot() ;

	void flushWriteState(WriteState &st);
	PInfo getDatabaseState(Handle h);
	PChangeset beginBatch(PInfo &nfo);
	void endBatch(PInfo &nfo);
	void value2document(const std::string_view &value, DocumentInfo &doc, bool only_header);

	mutable std::recursive_mutex lock;

	void loadViews();

	void deleteView(Handle h, ViewID view);

	ViewID allocView();



};

} /* namespace sofadb */

#endif /* SRC_LIBSOFA_DATABASECORE_H_ */
