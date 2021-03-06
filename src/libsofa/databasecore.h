/*
 * database.h
 *
 *  Created on: 5. 2. 2019
 *      Author: ondra
 */

#ifndef SRC_LIBSOFA_DATABASECORE_H_
#define SRC_LIBSOFA_DATABASECORE_H_

#include <map>
#include <set>
#include <vector>
#include <queue>
#include <memory>
#include "types.h"
#include "kvapi.h"
#include <mutex>
#include <functional>
#include <unordered_set>


namespace sofadb {
enum class Storage {
	permanent,
	memory
};

class DatabaseCore {
public:

	enum ObserverEvent {
		//a database has been created
		event_create,
		//a database content has been updated
		event_update,
		//a database has been destroyed (closed)
		event_close
	};

	typedef std::uint32_t Handle;
	static const Handle invalid_handle = static_cast<Handle>(-1);
	static const Handle memdb_mask = 0x80000000;
	static const Handle index_mask = 0x7FFFFFFF;

	typedef std::function<void()> Callback;
	typedef std::function<void(ObserverEvent, Handle, SeqNum)> Observer;
	typedef std::set<std::string> KeySet;



	struct DBConfig {
		///Size of log
		/** Log contains list of previous revision numbers allowing to easily connect
		 * new revision to an old revision without need to have all intermediate revisions. If log
		 * is small, that too many revision created outside this node will not be able replicated back, because
		 * connection to current revision will be lost (log overflows).
		 *
		 * Higher number costs performance and space. Each log item occupies extra 8 bytes of the document. These
		 * extra bytes must be also transfered during any lookup
		 */
		std::size_t logsize = 100;
		///Contains max age of stored revision. Older revisions are deleted. Value is in milliseconds
		/** Note that maintenance task will delete old revisions only when new revision is stored. Otherwise
		 * older revision survives much longer
		 *
		 */
		std::size_t history_max_age = 24*60*60*1000L; //30days
		///Specifies minimum count of older revisions stored as full copy
		/** Default value is 2, so total 3 revisions are stored. One current and two historical. They are
		 * kept stored even if they are older than history_max_age
		 */
		std::size_t history_min_count = 3; //keep 3 older revisions
		///specifies max limit of total stored revisions. This number must be above or equal to history_min_count
		/** This affects, how old revision will be removed
		 * First, all revisions above history_min_count and older than history_max_age are deleted.
		 * Second, if count of remaining old items is above this number, then some of revisions from now to max_age
		 * are also removed. Only immediate revision and oldest revision persists. So that means, that this
		 * number can't be less than 2 otherwise it wraps to 2.
		 *
		 * This number reduces count of historical revisions without loosing ability to perform 3-way merge
		 * with any conflicted revision which immediate parent revision is no longer available.
		 */
		std::size_t history_max_count = 8; //keep max 8 older revisions
		///Definex maximum count of old revisions for deleted documents
		/** Default value 0 means, that no older revision is stored, just tombstone, everything older is deleted. T
		 * This doesn't affect merge ability because deleted document is always winner unless there is update
		 * directly refers to deleted document's recent revision.
		 *
		 * Value 1 left one older revision that allows to do undelete operation and restore old data. Other values
		 * can make history larger, however settings for not-deleted document is still in effect also for
		 * deleted documents.
		 */
		std::size_t history_max_deleted = 0;
	};

	struct ChangeRec {
		std::string_view docid;
		RevID revid;
		SeqNum seqnum;
	};

	struct ViewResult {
		std::string_view docId;
		std::string_view key;
		std::string_view value;
	};

	using AlterKeyObserver = std::function<void(const std::string_view &)>;



private:
	struct WriteState {
		PChangeset curBatch;
		unsigned int lockCount = 0;
		std::queue<Callback> waiting;
	};

	struct ViewState {
		///name (identification) of the view
		std::string name;
		///sequence number of last update
		SeqNum seqNum = 0;
		///functions waiting to finish update
		std::queue<Callback> waiting;
		///true if the view is being updated
		bool updating = false;
	};

	using PViewState = std::unique_ptr<ViewState>;

	typedef std::vector<PViewState> ViewStateMap;
	typedef std::map<std::string, ViewID, std::less<> > ViewNameToID;

	struct Info {
		std::string name;
		SeqNum nextSeqNum = 1;
		SeqNum nextHistSeqNum = 1;
		Storage storage;

		DBConfig cfg;

		std::recursive_mutex lock;
		WriteState writeState;
		ViewStateMap viewState;
		ViewNameToID viewNameToID;

		ViewState *getViewState(std::size_t id) const {
			if (id < viewState.size()) return viewState[id].get();
			else return nullptr;
		}
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
		Info *operator->() const {return ptr;}
		bool operator==(nullptr_t) const {return ptr == nullptr;}
		bool operator!=(nullptr_t) const {return ptr != nullptr;}
		PInfo &operator=(const PInfo &other) {if (ptr) ptr->lock.unlock(); ptr = other.ptr;if (ptr) ptr->lock.lock();return *this;}
		PInfo &operator=(PInfo &&other) {if (ptr) ptr->lock.unlock(); ptr = other.ptr;other.ptr = nullptr;return *this;}
	};




public:

	DatabaseCore(PKeyValueDatabase db);


	Handle create(const std::string_view &name, Storage storage = Storage::permanent);
	Handle getHandle(const std::string_view &name) const;
	bool erase(Handle h);
	bool rename(Handle h, const std::string_view &newname);


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
		///Sequence number - field is used when document is read. It is ignored while document is written
		SeqNum seq_number;
		///Timestamp of last update
		/**The timestamp should change everytime the new revision is created */
		std::uint64_t timestamp;
		///Version number - note that only 7 bits are used
		unsigned char version;
		///true if document is marked as deleted
		bool deleted;
		///rest of the document is stored as json
		std::string_view payload;
	};

	class Lock {
		PInfo state;
	public:
		Lock(PInfo state):state(state) {}
		void unlock() {state = nullptr;}
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


	///Stores document to history, it doesn't change top
	/**
	 * @param h handle to database
	 * @param doc document to store
	 * @retval true stored
	 * @retval false already exists
	 */
	bool storeToHistory(Handle h, const RawDocument &doc);

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
	 * @param storage used to hold actual content of the document because RawDocument doesn't have space for the data
	 * @retval true found
	 * @retval false not found
	 */
	bool findDoc(Handle h, const std::string_view &docid, RawDocument &content, std::string &storage);

	///Retrieve historical document from the database
	/**
	 *
	 * @param h handle to databse
	 * @param docid document id
	 * @param revid revision id
	 * @param content this strutcure is filled by content
	 * @param storage used to hold actual content of the document because RawDocument doesn't have space for the data
	 * @retval true found
	 * @retval false not found
	 */
	bool findDoc(Handle h, const std::string_view &docid, RevID revid, RawDocument &content, std::string &storage);


	///Lists all revisions of the document
	/**
	 * @param h handle to the database
	 * @param docid document id
	 * @param callback function called for each revision
	 *
	 * @note revisions are not listed in the order. You need to sort them by seq_number
	 */
	bool enumAllRevisions(Handle h, const std::string_view &docid, std::function<void(const RawDocument &)> callback);


	///Erases doc
	/**
	 * This erases doc from the database. However it is not recomended use
	 * this function, because it can break replication. It is better to mark document
	 * as deleted (aka make tombstone). However it can be sometimes useful to erase
	 * old tombstones
	 *
	 * @param h
	 * @param docid
	 * @param reference to set which is filled by keys which has been modified by this operation.
	 * The modified keys must be used to update reduce maps
	 *
	 * @note function can take a time and block the database, because it is need
	 * to remove document from all views
	 */
	void eraseDoc(Handle h, const std::string_view &docid, KeySet &modifiedKeys);


	///Erases historical doc
	/**
	 * @param h handle to db
	 * @param docid document id
	 * @param revision revision id
	 *
	 * @note doesn't erase current revision.
	 */
	void eraseHistoricalDoc(Handle h, const std::string_view &docid, RevID revision);


	bool enumDocs(Handle h, const std::string_view &prefix,  bool reversed, std::function<bool(const RawDocument &)> callback);
	bool enumDocs(Handle h, const std::string_view &start_include, const std::string_view &end_exclude, std::function<bool(const RawDocument &)> callback);

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
	SeqNum readChanges(Handle h, SeqNum from, bool reversed, std::function<bool(const ChangeRec &)> &&fn);


	///Sets observer
	/** There can be one observer which is called on every update in any databse.
	 * The observer receives handle of the database
	 *
	 * @param observer
	 */
	void setObserver(Observer &&observer);


	///Destroyes everything removing all data to all databases and files from the disk
	/** Note function only marks database as destroyed. You need to also destroy the object
	 * with the database to operation take an effect.
	 *
	 * So after the function returns, you still can use the database. Also note that request is
	 * stored in memory, so leaving program without destroying the database object doesn't cause
	 * removing the database from the disk
	 */
	void destroy();


	bool getConfig(Handle h, DBConfig &cfg) ;

	bool setConfig(Handle h, const DBConfig &cfg);

	std::size_t getMaxLogSize(Handle h) ;

	using RevMap = std::unordered_map<RevID, bool>;

	///Clears history
	/**
	 * @param h handle to database
	 * @param docid document id
	 * @param revision_map contains map of relevant revisions. Revisions not included in
	 * 	this map are deleted. Revisions included in this map may be saved. If the
	 * 	revision is marked with true (as value in map), the revision is always saved.
	 * 	Other revisions are saved only if they are not too old depend on database configuration
	 * @return
	 */
	bool cleanHistory(Handle h, const std::string_view &docid, const RevMap &revision_map);

	///Prevents writes to other threads while the lock is held
	Lock lockWrite(Handle h);


	///Creates new view
	/**
	 * @param h handle to database
	 * @param name name of new view
	 * @return return handle to view or invalid_handle if there is already view with same name
	 */
	ViewID createView(Handle h, const std::string_view &name);

	///Erases view
	/**
	 * @param h handle to database
	 * @param viewId ID of view
	 * @retval true erased
	 * @retval false not found
	 */
	bool eraseView(Handle h, ViewID viewId);


	///Checks whether view needs update
	/**
	 *
	 * @param h db handle
	 * @param viewId view handle
	 * @retval true need update
	 * @retval false not need update
	 */
	bool view_needUpdate(Handle h, ViewID viewId);


	///Locks view for update
	/**
	 * @param h handle to database
	 * @param viewId handle to view
	 * @retval true locked
	 * @retval false not needed, view is already updated
	 */
	bool view_lockUpdate(Handle h, ViewID viewId);


	///Waits for update
	/**
	 * @param h handle to database
	 * @param viewId view id
	 * @param callback function will be called once the view is updated
	 * @retval true success
	 * @retval false failer - invalid handle or invalid viewId
	 */
	bool view_waitForUpdate(Handle h, ViewID viewId, Callback callback);


	bool view_finishUpdate(Handle h, ViewID viewId);

	///Updates view with new data
	/**
	 * @param h handle to database
	 * @param viewId handle of view
	 * @param seqNum sequence number. Update will be accepted only if seqNum is above last seqNum of the view
	 * @param docId document ID
	 * @param keyvaluedata contains array of key-values data. There are two strings for each update, where
	 * first of the strings is key and second is value. Note that key should not contain double zero
	 * characters otherwise it can be detected as field separator
	 * @param altered_keys keys that has been affected by this update (including that in keyvaluedata)
	 *
	 * @retval true update stored
	 * @retval false update failed - probably invalid h, invalid viewid or invalid seqNum
	 */
	bool view_updateDoc(Handle h, ViewID viewId, SeqNum seqNum,
			const std::string_view &docId,
			const std::basic_string_view<std::pair<std::string,std::string> > &keyvaluedata,
			AlterKeyObserver &&altered_keys);

	///Retrieves keys for single document of specified view
	/**
	 * @param h handle to database
	 * @param viewId handle of view
	 * @param docId document id
	 * @param callback function called for each result - it can return false to stop enumeration
	 * @return function returns sequence number of the view or 0 if error (because valid view cannot have seqnum 0)
	 */
	SeqNum view_getDocKeys(Handle h, ViewID viewId, std::string_view &docId, std::function<bool(const ViewResult &)> &&callback);

	///Retrieves list keys starting by given prefix
	/**
	 * @param h handle to database
	 * @param viewId handle to view
	 * @param prefix key prefix
	 * @param reversed set true to return in reversed order
	 * @param callback function called for every result
	 * @return function returns sequence number of the view or 0 of error
	 */
	SeqNum view_list(Handle h, ViewID viewId, std::string_view &prefix, bool reversed, std::function<bool(const ViewResult &)> &&callback);

	///Retrieve list of keys from given range
	SeqNum view_list(Handle h, ViewID viewId, std::string_view &start_key, std::string_view &end_key, std::function<bool(const ViewResult &)> &&callback);

	///Erase document from the view, returns modified keys
	/** this is for correct funtion of purge*/

	bool view_eraseDoc(Handle h, ViewID viewId, std::string_view &doc_id,
			AlterKeyObserver &&altered_keys);

	///Retrieves view's current sequence number
	SeqNum view_getSeqNum(Handle h, ViewID viewId);

	using ViewEmitFn = std::function<void(std::string_view, std::string_view)>;
	using ViewUpdateFn = std::function<void(const RawDocument &doc, const ViewEmitFn &)>;

	///Updates view
	/**
	 * @param h handle to database
	 * @param viewId handle to view
	 * @param limit maximum count of documents process in one call
	 * @param mapFn function called for every document to generate keys (to map document to keys)
	 * @param observer function that collects all altered keys
	 * @retval true update processed
	 * @retval false update not need or view doesn't exists
	 */
	bool view_update(Handle h, ViewID viewId, std::size_t limit, ViewUpdateFn &&mapFn, AlterKeyObserver &&observer);

protected:

	PKeyValueDatabase maindb,memdb;
	DBList dblist;
	NameToIDMap idmap;
	mutable std::recursive_mutex lock;
	Observer observer;




	Handle allocSlot() ;

	void flushWriteState(WriteState &st);
	PInfo getDatabaseState(Handle h);
	PChangeset beginBatch(const PInfo &nfo);
	void endBatch(const PInfo &nfo);
	void value2document(const std::string_view &value, RawDocument &doc);
	void document2value(std::string& value, const RawDocument& doc, SeqNum seqid);



	void deleteViewRaw(Handle h, ViewID view);

	ViewID allocView();

	void loadDBs();

	template<typename T>
	void storeProp(PInfo nfo, Handle h, std::string_view name, const T &prop);


	bool loadDBConfig(Handle h, DBConfig &cfg);
	bool storeDBConfig(Handle h, const DBConfig &cfg);

	void storeToHistory(PInfo dbf, Handle h, const RawDocument &doc);
	SeqNum getSeqNumFromDB(const std::string_view &prefix);

	PKeyValueDatabase selectDB(Handle h) const;
	PKeyValueDatabase selectDB(Storage storage) const;

	void loadDB(Iterator &iter);

	void view_eraseDoc2(Handle h, ViewID viewId, const PInfo &nfo,
			const std::string_view& doc_id, AlterKeyObserver&& altered_keys);


};

} /* namespace sofadb */

#endif /* SRC_LIBSOFA_DATABASECORE_H_ */
