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

	typedef std::function<void()> CloseBatchCallback;

private:
	struct WriteState {
		PChangeset curBatch;
		unsigned int lockCount = 0;
		std::queue<CloseBatchCallback> waiting;
	};

	struct Info {
		std::string name;
		SeqNum nextSeqNum = 1;
		WriteState writeState;
		///temporary buffer for key
		std::string key,key2;
		///temporary buffer for value
		std::string value,value2;

		std::recursive_mutex lock;
	};

	typedef std::map<std::string_view, Handle, std::less<> > NameToIDMap;
	typedef std::vector<std::unique_ptr<Info> > DBList;


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
		RevID revision;
		SeqNum seq_number;
		json::Value content;
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
	bool onBatchClose(Handle h, CloseBatchCallback cb);


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
	void enumAllRevisions(Handle h, const std::string_view &docid, std::function<void(const DocumentInfo &)> callback, bool only_header = false);


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



protected:

	PKeyValueDatabase maindb;
	DBList dblist;
	NameToIDMap idmap;


	Handle allocSlot() ;




};

} /* namespace sofadb */

#endif /* SRC_LIBSOFA_DATABASECORE_H_ */
