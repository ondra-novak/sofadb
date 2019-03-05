/*
 * replicationserver.cpp
 *
 *  Created on: 24. 2. 2019
 *      Author: ondra
 */

#include <libsofa/replicationserver.h>

namespace sofadb {


ReplicationServer::ReplicationServer(const DocumentDB &docdb, PEventRouter router, DatabaseCore::Handle h)
	:docdb(docdb),router(router),h(h),wh(1) {
}

ReplicationServer::~ReplicationServer() {
	this->stop();
}

void ReplicationServer::readManifest(SeqNum since, std::size_t limit,
		json::Value filter, bool longpoll,
		std::function<void(const Manifest&, SeqNum)>&& result) {

	Cdg _(cd);


	DatabaseCore &dbcore = docdb.getDBCore();
	std::string tmp;

	DocFilter flt = createFilter(filter);
	std::vector<DocRef> docs;
	SeqNum lastSeq = docdb.getDBCore().readChanges(h, since, false,
			[&](const DatabaseCore::ChangeRec &chrec){
		Cdg _(cd);
		if (flt != nullptr) {
			DatabaseCore::RawDocument rawdoc;
			if (!dbcore.findDoc(h,chrec.docid, chrec.revid, rawdoc, tmp)) return true;
			json::Value doc = DocumentDB::parseDocument(rawdoc,OutputFormat::replication);
			if (!flt(doc).defined()) return true;
		}
		docs.push_back(DocRef(std::string(chrec.docid),chrec.revid));
		return --limit > 0;
	});

	if (!docs.empty() || !longpoll) {
		result(Manifest(docs.data(),docs.size()), lastSeq);
	} else {
		auto cb = [result = std::move(result),this,filter,lastSeq,limit](bool) mutable {
			readManifest(lastSeq,limit,filter,true,std::move(result));
		};
		EventRouter::WaitHandle nwh = router->waitForEvent(h,lastSeq,24*60*60*1000, std::move(cb));
		if (wh.exchange(nwh) == 0) {
			router->cancelWait(wh);
		}

	}

}

void ReplicationServer::downloadDocs(const DownloadRequest& dwreq,
		std::function<void(const DocumentList&)>&& callback) {

	Cdg _(cd);

	std::vector<json::Value> lst;
	DatabaseCore &dbcore = docdb.getDBCore();
	std::string tmp;

	for (auto &&c : dwreq) {
		DatabaseCore::RawDocument rawdoc;
		if (dbcore.findDoc(h,c.id, c.rev, rawdoc, tmp)) {
			json::Value v = DocumentDB::parseDocument(rawdoc,OutputFormat::replication);
			lst.push_back(v);
		}
		else {
			lst.push_back(nullptr);
		}
	}
	callback(DocumentList(lst.data(),lst.size()));
}

void ReplicationServer::downloadDocs(const DownloadTopRequest& dwreq,
		std::function<void(const DocumentList&)>&& callback) {

	Cdg _(cd);

	std::vector<json::Value> lst;
	DatabaseCore &dbcore = docdb.getDBCore();
	std::string tmp;

	for (auto &&c : dwreq) {
		DatabaseCore::RawDocument rawdoc;
		if (dbcore.findDoc(h,c, rawdoc, tmp)) {
			json::Value v = DocumentDB::parseDocument(rawdoc,OutputFormat::replication);
			lst.push_back(v);
		}
		else {
			lst.push_back(nullptr);
		}
	}
	callback(DocumentList(lst.data(),lst.size()));
}

void ReplicationServer::uploadHistoricalDocs(const DocumentList& documents,
		std::function<void(const PutStatusList&)>&& callback) {

	Cdg _(cd);

	std::vector<PutStatus> st;
	for (auto &&c: documents) {
		st.push_back(docdb.replicator_put_history(h,c));
	}
	callback(PutStatusList(st.data(),st.size()));
}

void ReplicationServer::stop() {
	std::size_t x = wh.exchange(0);
	if (x) router->cancelWait(x,false);
	cd.wait();

}

void ReplicationServer::sendManifest(const Manifest& manifest,
						std::function<void(const DownloadRequest&)>&& callback) {

	Cdg _(cd);

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

	Cdg _(cd);

	std::vector<PutStatus> st;
	json::String dummy;
	for (auto &&c: documents) {
		st.push_back(docdb.replicator_put(h,c,dummy));
	}
	callback(PutStatusList(st.data(),st.size()));

}

void ReplicationServer::resolveConflicts(const DocumentList &documents,
				std::function<void(const DocumentList &)> &&callback) {

	Cdg _(cd);

	std::vector<json::Value> result;

	for (auto &&doc: documents) {
		json::Value merged;
		docdb.resolveConflict(h,doc,merged);
		if (merged.defined())
			result.push_back(merged);
	}
	callback(DocumentList(result.data(),result.size()));
}

void ReplicationServer::setWarningCallback(WarningCallback &&callback) {
	Cdg _(cd);
	wcb = std::move(callback);
}

} /* namespace sofadb */
