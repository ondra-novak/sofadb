/*
 * replicationtask.cpp
 *
 *  Created on: 28. 2. 2019
 *      Author: ondra
 */

#include <libsofa/replicationtask.h>

namespace sofadb {

ReplicationTask::ReplicationTask(IReplicationProtocol *source, IReplicationProtocol *target)
	:source(source),target(target), started(false), stopcond(nullptr)
{
	// TODO Auto-generated constructor stub

}

ReplicationTask::~ReplicationTask() {
	stop();
}


void ReplicationTask::start(SeqNum since, json::Value filter, bool continuous) {

	Sync _(lock);

	if (started) return;
	started = true;

	this->seqnum = since;
	this->filter = filter;
	this->continuous = continuous;
	_.unlock();

	doWork(10);


}

void ReplicationTask::doWork(std::size_t limit) {
	if (!started) return;
	async_detect = 1;
	this->old_seqnum = this->seqnum;
	source->readManifest(seqnum, limit,filter, continuous, [=](IReplicationProtocol::Manifest m, SeqNum seqnum){
		if (m.empty()) {
			finish();
			return;
		}
		if (checkStop()) return;
		this->seqnum = seqnum;
		target->sendManifest(m, [=](IReplicationProtocol::DownloadRequest req) {
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
						target->uploadDocs(docs, [=,doclist=std::move(doclist)](IReplicationProtocol::PutStatusList status){
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
	if (--async_detect < 0) doWork(10);
}

void ReplicationTask::retryWork() {
	if (--async_detect < 0) doWork(1000);
}

void ReplicationTask::stop() {
	Sync _(lock);
	if (started) {
		std::condition_variable cond;
		stopcond = &cond;
		continuous = false;
		source->stopRead();
		cond.wait(_);
	}
}

bool ReplicationTask::isRunning() const {
	Sync _(lock);
	return started;
}

void ReplicationTask::onFinish() {
	//nothing here
}

bool ReplicationTask::onError(json::Value , PutStatus st) {
	if (st == PutStatus::db_not_found) return false;
	return true;
}

bool ReplicationTask::checkStop() {
	Sync _(lock);
	if (stopcond != nullptr) {
		stopcond->notify_all();
		this->seqnum = this->old_seqnum;
		started = false;
		return true;
	} else {
		return false;
	}
}
void ReplicationTask::finish() {
	this->started = false;
	checkStop();
	onFinish();
}



} /* namespace sofadb */
