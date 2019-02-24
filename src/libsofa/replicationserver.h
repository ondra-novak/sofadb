/*
 * replicationserver.h
 *
 *  Created on: 24. 2. 2019
 *      Author: ondra
 */

#ifndef SRC_LIBSOFA_REPLICATIONSERVER_H_
#define SRC_LIBSOFA_REPLICATIONSERVER_H_

#include "docdb.h"
#include "databasecore.h"
#include "eventrouter.h"
#include "replication.h"
#include <condition_variable>
#include <mutex>

namespace sofadb {

class ReplicationServer: public IReplicationProtocol {
public:
	ReplicationServer(const DocumentDB &dbcore, PEventRouter router, DatabaseCore::Handle h);
	~ReplicationServer();

	virtual void readManifest(SeqNum since,
			std::size_t limit,
			json::Value filter,
			bool longpoll,
			std::function<void(const Manifest &, SeqNum)> &&result);
	virtual void downloadDocs(const DownloadRequest &dwreq,
			std::function<void(const DocumentList &)> &&callback);
	virtual void stopRead();
	virtual void sendManifest(const Manifest &manifest,
			std::function<void(const DownloadRequest &)> &&callback);
	virtual void uploadDocs(const DocumentList &documents,
			std::function<void(const PutStatusList &)> &&callback);

protected:
	DocumentDB docdb;
	PEventRouter router;
	DatabaseCore::Handle h;
	EventRouter::WaitHandle wh = 0;
	std::mutex lock;
	std::condition_variable *stopntf = nullptr;
	using Sync = std::unique_lock<std::mutex>;

	void stopReadLk(Sync &_);

	virtual void readManifestLk(SeqNum since,
			std::size_t limit,
			json::Value filter,
			bool longpoll,
			std::function<void(const Manifest &, SeqNum)> &&result);

};

} /* namespace sofadb */

#endif /* SRC_LIBSOFA_REPLICATIONSERVER_H_ */
