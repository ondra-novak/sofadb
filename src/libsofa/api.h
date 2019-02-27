/*
 * api.h
 *
 *  Created on: 13. 2. 2019
 *      Author: ondra
 */

#ifndef SRC_LIBSOFA_API_H_
#define SRC_LIBSOFA_API_H_

#include <imtjson/value.h>
#include "kvapi.h"
#include "databasecore.h"
#include "docdb.h"
#include "eventrouter.h"
#include "replication.h"
#include "filter.h"
#include "maintenancetask.h"

namespace sofadb {


///This class contains end-user API to control content of SofaDB
class SofaDB{
public:

	SofaDB(PKeyValueDatabase kvdatabase);
	SofaDB(PKeyValueDatabase kvdatabase, Worker worker);
	~SofaDB();

	using Handle = DatabaseCore::Handle;
	using Filter = std::function<json::Value(const json::Value &)>();
	using ResultCB = DocumentDB::ResultCB;
	using WaitHandle = EventRouter::WaitHandle;
	using Observer = EventRouter::Observer;
	using GlobalObserver = EventRouter::GlobalObserver;
	using ObserverHandle = EventRouter::ObserverHandle;

	static const Handle invalid_handle = DatabaseCore::invalid_handle;


	///Creates database
	/**
	 * @param name name of the databse
	 * @return handle to newly created database
	 */
	Handle createDB(const std::string_view &name, Storage storage);
	///Finds database by a name
	/**
	 * @param name name of the databse
	 * @return handle. If database doesn't exists, function returns invalid_handle
	 */
	Handle getDB(const std::string_view &name);
	///Deletes database
	/**
	 * @param h handle to database
	 * @retval true deleted
	 * @retval false not found
	 *
	 *
	 * @note database can deleted asynchronously
	 */
	bool deleteDB(Handle h);
	///Renames databse
	/**
	 * @param h database handle
	 * @param name new name
	 * @retval true renamed
	 * @retval false error
	 *
	 * Function just changes the name, the handle doesn't change
	 */
	bool renameDB(Handle h, const std::string_view &name);
	template<typename Fn> auto listDB(Fn &&fn) {return dbcore.list(std::forward<Fn>(fn));}


	///List all documents in database
	/**
	 * @param db database handle
	 * @param outputFormat specify output format
	 * @param cb callback function which is called for every result. The callback can return false to stop reading
	 * @retval true processed
	 * @retval false stopped or not found
	 */
	bool allDocs(Handle db, OutputFormat outputFormat, ResultCB &&cb);
	///List all documents in database
	/**
	 * @param db database handle
	 * @param outputFormat specify output format
	 * @param prefix name prefix
	 * @param cb callback function which is called for every result. The callback can return false to stop reading
	 * @retval true processed
	 * @retval false stopped or not found
	 */
	bool allDocs(Handle db, OutputFormat outputFormat, const std::string_view &prefix, ResultCB &&cb);
	///List all documents in database
	/**
	 * @param db database handle
	 * @param outputFormat specify output format
	 * @param prefix name prefix
	 * @param reversed set true to return results in reversed order
	 * @param cb callback function which is called for every result. The callback can return false to stop reading
	 * @retval true processed
	 * @retval false stopped or not found
	 */
	bool allDocs(Handle db, OutputFormat outputFormat, const std::string_view &prefix, bool reversed, ResultCB &&cb);
	///List part of documents in database
	/**
	 * @param db database handle
	 * @param outputFormat output format
	 * @param start_key start key, which is also included into results
	 * @param end_key end key, which is not included into result. If start_key > end_key, result is reversed
	 * @param cb callback function which is called for every result. The callback can return false to stop reading
	 * @retval true processed
	 * @retval false stopped or not found
	 */
	bool allDocs(Handle db, OutputFormat outputFormat, const std::string_view &start_key, const std::string_view &end_key, ResultCB &&cb);


	///Puts document to the database
	/**
	 * @param db database handle
	 * @param doc document to put
	 * @param newrev receives new revision as json object
	 * @return if 'stored' returned, then document has been stored otherwise it can contain 'conflict' or
	 *   other error.
	 *
	 * @note Function can internally merge the result. However it returns revision of nonmerged result.
	 * If the next put referes to nonmerged revision, it will be also merged with top revision efectivelly
	 * creating separate branch. The only way to close that branch is to put new revision to top
	 * of revision
	 */
	PutStatus put(Handle db, const json::Value &doc, json::String &newrev);
	///Replicates document from other database
	/**
	 * @param db database handle
	 * @param doc document from other database. It must have revision log
	 * @param history set true to store document to the history - this doesn't change current top document
	 * @return  if 'stored' returned, then document has been stored otherwise it can contain 'conflict' or
	 *   other error.
	 *
	 */
	PutStatus replicatorPut(Handle db, const json::Value &doc, bool history);
	///Retrieves document from the database
	/**
	 * @param h handle to database
	 * @param id document ID
	 * @param format output format
	 * @return document object. If the document is not found, then undefined is returned
	 */
	json::Value get(Handle h, const std::string_view &id, OutputFormat format);
	///Retrieves document from the database of the given revision
	/**
	 *
	 * @param h handle to database
	 * @param id document ID
	 * @param rev revision ID
	 * @param format output format
	 * @return document object. If the document is not found, then undefined is returned
	 */
	json::Value get(Handle h, const std::string_view &id, const std::string_view &rev, OutputFormat format);

	///Erases document (sets is deleted)
	/**
	 * @param h handle to database
	 * @param docid document id
	 * @param revid revision id
	 *
	 */
	PutStatus erase(Handle h, const std::string_view &docid, const std::string_view &revid);

	///Purges historical revision
	void purge(Handle h, const std::string_view &docid, const std::string_view &revid);

	///Purges whole document
	/**
	 * @note Purge is not reported to the changes. Purge cannot be replicated. Purge can break replication
	 *
	 * @param h handle to database
	 * @param docid document id
	 */
	void purge(Handle h, const std::string_view &docid);

	///Reads changes of the database
	/**
	 * @param h handle of database
	 * @param since id of last known change. To read changes from the beginning, use 0
	 * @param format specify output format
	 * @param callback function called for every result
	 * @retval true all changes reported (or none)
	 * @retval false canceled during processing
	 */
	SeqNum readChanges(Handle h, SeqNum since, bool reversed, OutputFormat format, ResultCB &&callback);
	///Reads changes of the database
	/**
	 * @param h handle of database
	 * @param since id of last known change. To read changes from the beginning, use 0
	 * @param format specify output format
	 * @param callback function called for every result
	 * @retval true all changes reported (or none)
	 * @retval false canceled during processing
	 */
	SeqNum readChanges(Handle h, SeqNum since, bool reversed, OutputFormat format, DocFilter &&flt, ResultCB &&callback);


	///Waits for new changes (one shot)
	/**
	 * @param h handle of database
	 * @param since id of last known change.
	 * @param timeout_ms timeout in milliseconds
	 * @param observer function called on change or timeout. The function receives boolean
	 * 	argument - true, change detected or false, timeout or error
	 * @return function returns handle which can be useful to cancel asynchronous waiting.
	 *
	 *
	 * @note if there is already change detected, the function returns value, which can be
	 * checked with operator !. This operator returns true, when no waiting is needed, because
	 * there are already changes to read.
	 *
	 * @code
	 * WaitHandle wh = api.waitForChanges(h,since,timeout,observer);
	 * if (!wh) {
	 *     //read changes now
	 * } else {
	 *     //observer will be notified
	 * }
	 * @endcode
	 *

	 */
	WaitHandle waitForChanges(Handle h, SeqNum since, std::size_t timeout_ms, Observer &&observer);


	///Cancels any waiting
	/**
	 * @param wh handle value returned by waitForChanges
	 * @retval true canceled
	 * @retval false handle is no longer valid, probabily already triggered
	 */
	bool cancelWaitForChanges(WaitHandle wh, bool notify_fn = false);

	///Register global observer
	/**
	 * @param observer observer function
	 * @return observer handle
	 *
	 * @note observer is not one-shot. It is triggered for every change until it is removed
	 */
	ObserverHandle registerObserver(GlobalObserver &&observer);

	///Removes global observer
	/**
	 * @param handle handle of observer
	 * @retval true removed
	 * @retval false not found
	 *
	 * @note the observer still can receive events after function returns, there
	 * can be already stacked notification in the router's queue. There is no
	 * function to flush that queue
	 */
	bool removeObserver(ObserverHandle handle);




	IReplicationProtocol *createReplicationServer(Handle h);

public:

	DatabaseCore &getDBCore();
	DocumentDB &getDocDB();
	PEventRouter getEventRouter();


protected:


	DatabaseCore dbcore;
	DocumentDB docdb;
	PEventRouter eventRouter;
	MaintenanceTask mtask;

};





}


#endif /* SRC_LIBSOFA_API_H_ */
