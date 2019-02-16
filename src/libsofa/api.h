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

namespace sofadb {


///This class contains end-user API to control content of SofaDB
class SofaDB {
public:

	SofaDB(PKeyValueDatabase kvdatabase);
	SofaDB(PKeyValueDatabase kvdatabase, Worker worker);

	using Handle = DatabaseCore::Handle;
	using Filter = std::function<json::Value(const json::Value &)>();
	using ResultCB = DocumentDB::ResultCB;
	using WaitHandle = EventRouter::WaitHandle;
	using GlobalObserver = EventRouter::GlobalObserver;
	using ObserverHandle = EventRouter::ObserverHandle;

	static const Handle invalid_handle = DatabaseCore::invalid_handle;


	///Creates database
	/**
	 * @param name name of the databse
	 * @return handle to newly created database
	 */
	Handle createDB(const std::string_view &name);
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


	///monitors changes
	/**
	 * Function performs monitoring of changes asynchronously
	 *
	 * @param h database handle
	 * @param since sequence number of last seen update.
	 * @param outputFormat output format
	 * @param timeout timeout for waiting new change. If set to zero, function returns immediatelly
	 * @param callback function called when new change is detected. If there is already a change
	 *                 recorder, the callback is called in current thread. Otherwise, the callback
	 *                 is registered and monitoring continues asynchronously.
	 *
	 *                 The callback function must return true to continue monitoring, or false to
	 *                 stop monitoring. In case of timeout, function receives null instead of value
	 *                 as the last result.
	 *
	 * @return function returns nullptr when operation has been processed synchronously. Function returns
	 *    a wait handle if operation will be processed asynchronously. You can use the handle to
	 *    function cancelMonitor
	 */
	WaitHandle monitorChanges(Handle h,
				SeqNum since,
				OutputFormat outputFormat,
				std::size_t timeout,
				ResultCB callback);



	///Cancels monitoring
	/**
	 * @param wh handle returned by monitorChanges()
	 * @retval true canceled
	 * @retval false invalid handle (probably already executed)
	 */
	bool cancelMonitor(WaitHandle wh);


	ObserverHandle registerObserver(GlobalObserver &&observer);

	bool removeObserver(ObserverHandle handle);

	DatabaseCore &getDBCore();

public:

	DatabaseCore &getDatabaseCore();
	DocumentDB &getDocumentDB();
	EventRouter &getEventRouter();


protected:


	DatabaseCore dbcore;
	DocumentDB docdb;
	PEventRouter eventRouter;

};




}


#endif /* SRC_LIBSOFA_API_H_ */
