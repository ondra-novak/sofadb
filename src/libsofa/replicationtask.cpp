/*
 * replicationtask.cpp
 *
 *  Created on: 28. 2. 2019
 *      Author: ondra
 */

#include <libsofa/replicationtask.h>

namespace sofadb {

ReplicationTask::ReplicationTask(PProtocol &&source, PProtocol &&target)
	:source(std::move(source)),target(std::move(target)), started(false)
{
	this->source->setWarningCallback([=](int code, std::string &&message){
		this->onWarning(Side::source,code,std::move(message));
	});
	this->target->setWarningCallback([=](int code, std::string &&message){
		this->onWarning(Side::target,code,std::move(message));
	});

}

ReplicationTask::~ReplicationTask() {
	stop();
}


void ReplicationTask::start(SeqNum since, json::Value filter, bool continuous) {


	if (started) return;
	started = true;
	stopSignal = false;

	this->seqnum = since;
	this->filter = filter;
	this->continuous = continuous;


	doWork(initial_batch_size);


}

void ReplicationTask::doWork(std::size_t limit) {
	if (!started) return;
	do {
		async_detect = 1;
		this->old_seqnum = this->seqnum;
		source->readManifest(seqnum, limit,filter, continuous, [=,g=Grd(exitWait)](IReplicationProtocol::Manifest m, SeqNum seqnum){
			if (checkStop()) return;
			this->seqnum = seqnum;
			target->sendManifest(m, [=,g=Grd(exitWait)](IReplicationProtocol::DownloadRequest req) {
				if (checkStop()) return;
				if (!req.empty()) {
					source->downloadDocs(req,[=](IReplicationProtocol::DocumentList docs) {
						std::vector<json::Value> doclist;
						if (checkStop()) return;
						doclist.reserve(docs.size());
						for (auto &&c: docs) {
							if (c != nullptr) doclist.push_back(c);
						}
						if (!doclist.empty()) {
							target->uploadDocs(docs, [=,doclist=std::move(doclist),g=Grd(exitWait)](IReplicationProtocol::PutStatusList status){
								std::size_t cnt = doclist.size();
								std::vector<json::Value> conflicts;
								for (std::size_t i = 0; i < cnt; i++) {
									if (status[i] == PutStatus::conflict) {
										conflicts.push_back(docs[i]);
									} else if (status[i] != PutStatus::stored) {
										if (!onError(doclist[i],status[i])) {
											finish();
											return;
										}
									}
								}
								if (!conflicts.empty()) {
	//								resolveConflicts(conflicts);
								}
								else {
									retryWork();
								}
							});
						} else {
							retryWork();
						}
					});
				} else {
					retryWork();
				}
			});
		});
	}
	while (--async_detect < 0);
	onUpdate(this->seqnum);
}

void ReplicationTask::onUpdate(SeqNum) {
	//nothing here
}

void ReplicationTask::onWarning(Side , int, std::string&& ) {
	//nothing here
}

void ReplicationTask::retryWork() {
	if (--async_detect < 0) {
		doWork(batch_size);
	}
}

void ReplicationTask::stop() {
	if (started) {
		stopSignal = true;
		continuous = false;
		source->stop();
		target->stop();
		exitWait.wait();
	}
}

bool ReplicationTask::isRunning() const {
	return started;
}

void ReplicationTask::onFinish(SeqNum) {
	//nothing here
}

bool ReplicationTask::onError(json::Value , PutStatus st) {
	if (st == PutStatus::db_not_found) return false;
	return true;
}

bool ReplicationTask::checkStop() {
	if (stopSignal) {
		return true;
	} else {
		return false;
	}
}
void ReplicationTask::finish() {
	this->started = false;
	checkStop();
	onFinish(this->seqnum);
}



} /* namespace sofadb */
