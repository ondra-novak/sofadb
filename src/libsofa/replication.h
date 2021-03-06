/*
 * replication.h
 *
 *  Created on: 14. 2. 2019
 *      Author: ondra
 */

#ifndef SRC_LIBSOFA_REPLICATION_H_
#define SRC_LIBSOFA_REPLICATION_H_
#include <imtjson/string.h>
#include <string_view>



namespace sofadb {

///This interface defines replication protokol
/** The methods here are called by a replication client and they are implemented by replication
 * server. The implementation can be also provided through the network connection. For both
 * sides transfer method is transparent
 *
 *
 *
 * Replication
 * -------------
 * TWO modes
 *
 * 1) client --> server
 * 2) client <-- server
 *
 * The replication is always controlled by the client. Server just carries out the commands
 *
 * 1) mode client --> server
 * 1.1) client generates manifest of chanegs. For first replication, it generates manifest of
 * while database, other replications requires only changes
 * 1.2) client sends manifest to the server
 * 1.3) server returns list of documents that are looks newest by the manifest (it can also
 *      request conflicted documents in order to merge)
 * 1.4) client sends each requested document and receives status
 * 1.5) in case, that conflict is detected, client recives top level document in order to merge
 *
 * 2) mode client <-- server
 * 2.1) client requests the manifest since given seqnum
 * 2.2) client receives manifest and finds updated documents
 * 2.3) client requests these documents
 * 2.4) client receives these documents and stores them
 *
 */


class IReplicationProtocol {
public:

	struct DocRef {
		std::string id;
		RevID rev;
		DocRef(std::string &&id,RevID rev):id(std::move(id)),rev(std::move(rev)) {}
		DocRef(const std::string &id,RevID rev):id(id),rev(rev) {}
		DocRef() {}
	};

	static const SeqNum error = SeqNum(-1);


	using WarningCallback = std::function<void(int code, std::string &&)>;
	using DownloadRequest = std::basic_string_view<DocRef>;
	using DownloadTopRequest = std::basic_string_view<std::string>;
	using Manifest = std::basic_string_view<DocRef>;
	using DocumentList = std::basic_string_view<json::Value>;
	using PutStatusList = std::basic_string_view<PutStatus>;
	using WaitHandle = std::size_t;


	///Reads manifest from the source
	/**
	 * Manifest contains list of documents changed from the last call.
	 * @param since sequence number of last call. For the very first call, use 0
	 * @param limit maximum count of items to download
	 * @param filter specifies filter (for filtered replication). The filter can be either
	 *    json native filter or filter name on a filter defined in source database. The
	 *    value can be null/undefined to disable filtering
	 * @param longpoll if set true, the callback function is called for every change on source database
	 *  until it is stopped by function stopRead(). If this is false, the callback
	 *  function called once
	 * @param result function called when result is available. The function accepts two
	 * arguments. First argument is manifest, second contains last processed SeqNum. If the
	 * second argument equals to IReplicationProtocol::error, then an error happen. You
	 * can use std::current_exception() to receive reason. If the longpoll is true, the
	 * function is called for every change made on the source database until the reading
	 * is stopped or an error is reported
	 *
	 */
	virtual void readManifest(SeqNum since,
			std::size_t limit,
			json::Value filter,
			bool longpoll,
			std::function<void(const Manifest &, SeqNum)> &&result) = 0;

	///Downloads documents from the source
	/**
	 * @param dwreq request contains list of documents to download
	 * @param callback function is called when operation completes. If this argument is empty,
	 * there is error reported
	 */
	virtual void downloadDocs(const DownloadRequest &dwreq,
			std::function<void(const DocumentList &)> &&callback) = 0;

	///Downloads top level versions of the documents
	/**
	 * Used to download current revision of documents for merge
	 *
	 * @param dwreq request contains list of documents to download
	 * @param callback function is called when operation completes. If this argument is empty,
	 * there is error reported
	 */
	virtual void downloadDocs(const DownloadTopRequest &dwreq,
			std::function<void(const DocumentList &)> &&callback) = 0;

	///Stops any current running operation
	/** Function must block until the operation is stopped */
	virtual void stop() = 0;

	///Sends manifest to the target database
	/** Function sends manifest so target database can filter the manifest and requests
	 * documents to replicate
	 * @param manifest manifest
	 * @param callback called once the result is complete
	 */
	virtual void sendManifest(const Manifest &manifest,
			std::function<void(const DownloadRequest &)> &&callback) = 0;

	///Sends documents to the target database
	/**
	 *
	 * @param documents
	 * @param callback
	 */
	virtual void uploadDocs(const DocumentList &documents,
			std::function<void(const PutStatusList &)> &&callback) = 0;

	///Uploads docs to store in historical database
	/**
	 * It is used to store conflicted revision on the target before it is merged. These documents are stored in history and never shows up as top level
	 *
	 * @param documents documents to store
	 * @param callback callback function
	 */
	virtual void uploadHistoricalDocs(const DocumentList &documents,
			std::function<void(const PutStatusList&)> &&callback) = 0;


	///Requests to resolve conflicts on documents
	/**
	 * @param documents list of documents containing top-level documents on the other side.
	 * @param callback function called with resolved documents. Resolved documents should mention local top-level document and received document
	 * in log record. It is also possible put one of the documents to conflict record and keep document conflicted. The returned
	 * documents are uploaded to other side. If the upload fails with conflict, operation can repeat with new set of documents
	 *
	 * Conflict resolution is not stored, it only uses data of local database to resolve conflict
	 */
	virtual void resolveConflicts(const DocumentList &documents,
				std::function<void(const DocumentList &)> &&callback) = 0;


	///Registers callbacks to deliver warning appears during communication
	/** Warnings can help to find problems during replication - because the
	 * replication protocol should be robust and stable, it won't probably generate
	 * errors. If there is connection issue, the protocol will try to reconnect and
	 * restore connection as soon as possible without any retry limit. However to
	 * give user notice about this issue, it can send a warning  to this function
	 *
	 * @param callback
	 */
	virtual void setWarningCallback(WarningCallback &&callback) = 0;

	virtual ~IReplicationProtocol() {}
};

}




#endif /* SRC_LIBSOFA_REPLICATION_H_ */
