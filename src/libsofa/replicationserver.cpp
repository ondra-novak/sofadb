/*
 * replicationserver.cpp
 *
 *  Created on: 24. 2. 2019
 *      Author: ondra
 */

#include <libsofa/replicationserver.h>

namespace sofadb {


ReplicationServer::ReplicationServer(const DocumentDB &docdb, PEventRouter router, DatabaseCore::Handle h)
	:docdb(docdb),router(router),h(h) {
}

ReplicationServer::~ReplicationServer() {
	this->stopRead();
}

void ReplicationServer::readManifest(SeqNum since, std::size_t limit,
		json::Value filter, bool longpoll,
		std::function<void(const Manifest&, SeqNum)>&& result) {

	Sync _(lock);
	if (wh != 0) {
		stopReadLk(_);
	}

	readManifestLk(since,limit,filter,longpoll,std::move(result));
}
void ReplicationServer::readManifestLk(SeqNum since, std::size_t limit,
		json::Value filter, bool longpoll,
		std::function<void(const Manifest&, SeqNum)>&& result) {

	std::vector<ManifestItem> r;

	SeqNum lastSeq = docdb.readChanges(h, since, false, OutputFormat::log|OutputFormat::deleted,
			createFilter(filter), [&](json::Value doc){
		r.push_back(ManifestItem ({doc["id"].toString(), doc["rev"].toString(), doc["log"]}));
		return --limit > 0;
	});

	if (!r.empty()) {
		result(Manifest(r.data(), r.size()), lastSeq);
	}

	if (longpoll) {
		auto cb = [result = std::move(result),this,filter,lastSeq,limit](bool) mutable {
			Sync _(lock);
			if (stopntf) {
				stopntf->notify_all();
				return;
			} else {
				readManifestLk(lastSeq,limit,filter,true,std::move(result));
			}
		};
		EventRouter::WaitHandle nwh = router->waitForEvent(h,lastSeq,24*60*60*1000, std::move(cb));
		if (nwh == 0) {
			router->dispatch([cb = std::move(cb)]() mutable {cb(true);});
		}

	}

}

void ReplicationServer::downloadDocs(const DownloadRequest& dwreq,
		std::function<void(const DocumentList&)>&& callback) {
}


void ReplicationServer::stopReadLk(Sync &_) {
	std::condition_variable cond;
	stopntf = &cond;
	router->cancelWait(wh,true);
	cond.wait(_);
	stopntf = nullptr;
	wh = 0;

}
void ReplicationServer::stopRead() {
	Sync _(lock);
	if (wh != 0) {
		stopReadLk(_);
	}

}

void ReplicationServer::sendManifest(const Manifest& manifest,
		std::function<void(const DownloadRequest&)>&& callback) {
}

void ReplicationServer::uploadDocs(const DocumentList& documents,
		std::function<void(const PutStatusList&)>&& callback) {
}

} /* namespace sofadb */
