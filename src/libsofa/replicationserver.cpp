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


	DatabaseCore &dbcore = docdb.getDBCore();
	std::string tmp;

	DocFilter flt = createFilter(filter);
	std::vector<DocRef> docs;
	SeqNum lastSeq = docdb.getDBCore().readChanges(h, since, false,
			[&](const DatabaseCore::ChangeRec &chrec){
		if (flt != nullptr) {
			DatabaseCore::RawDocument rawdoc;
			if (!dbcore.findDoc(h,chrec.docid, chrec.revid, rawdoc, tmp)) return true;
			json::Value doc = DocumentDB::parseDocument(rawdoc,OutputFormat::data_and_log_and_deleted);
			if (!flt(doc).defined()) return true;
		}
		docs.push_back(DocRef(std::string(chrec.docid),chrec.revid));
		return --limit > 0;
	});

	if (!docs.empty()) {
		result(Manifest(docs.data(),docs.size()), lastSeq);
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

	std::vector<json::Value> lst;
	DatabaseCore &dbcore = docdb.getDBCore();
	std::string tmp;

	for (auto &&c : dwreq) {
		DatabaseCore::RawDocument rawdoc;
		if (dbcore.findDoc(h,c.id, c.rev, rawdoc, tmp)) {
			json::Value v = DocumentDB::parseDocument(rawdoc,OutputFormat::data_and_log_and_deleted);
			lst.push_back(v);
		}
		else {
			lst.push_back(nullptr);
		}
	}
	callback(DocumentList(lst.data(),lst.size()));
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

	std::string tmp;
	DatabaseCore &dbcore = docdb.getDBCore();
	std::vector<DocRef> request;

	for (auto &&c: manifest) {
		DatabaseCore::RawDocument rawdoc;
		if (dbcore.findDoc(h,c.id, rawdoc, tmp)) {
			if (c.rev == rawdoc.revision) continue;
			std::string_view p = rawdoc.payload;
			json::Value log = DocumentDB::parseLog(p);
			if (log.indexOf(c.rev) != json::Value::npos) continue;
		}
		request.push_back(c);
	}

	callback(DownloadRequest(request.data(),request.size()));
}

void ReplicationServer::uploadDocs(const DocumentList& documents,
						std::function<void(const PutStatusList&)>&& callback) {

	std::vector<PutStatus> st;
	for (auto &&c: documents) {
		st.push_back(docdb.replicator_put(h,c));
	}
	callback(PutStatusList(st.data(),st.size()));

}

} /* namespace sofadb */
