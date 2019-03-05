/*
 * maintenancetask.cpp
 *
 *  Created on: 20. 2. 2019
 *      Author: ondra
 */

#include <imtjson/value.h>
#include <libsofa/keyformat.h>
#include <shared/logOutput.h>
#include "maintenancetask.h"
#include "docdb.h"

using ondra_shared::logInfo;

namespace sofadb {


using ondra_shared::logInfo;
using namespace json;

MaintenanceTask::MaintenanceTask(DatabaseCore& dbcore):dbcore(dbcore) {
}

void MaintenanceTask::init(PEventRouter router) {


	if (this->router != nullptr) {
		this->router->removeObserver(this->oh);
	}
	this->router = router;
	std::vector<std::pair<Handle,SeqNum> > dblist;
	router->listDBs([&](auto &&item) {dblist.push_back(item);return true;});
	for (auto &&item:dblist) {
		Handle h = item.first;
		SeqNum n = item.second;
		init_task(router,h,n);
	}


	this->oh = this->router->registerObserver([=,g=Sync(cntd)](DatabaseCore::ObserverEvent ev, Handle h, SeqNum sq){
		if (ev == DatabaseCore::event_create) {
			init_task(router,h,sq);
		}
		return true;
	});
}

bool MaintenanceTask::init_rev_map(DatabaseCore::RevMap &revision_map,
			Handle h, const std::string_view &id) {
	std::string tmp;
	DatabaseCore::RawDocument rawdoc;
	if (dbcore.findDoc(h,id,rawdoc,tmp)) {

		std::string_view payload = rawdoc.payload;
		Value log = Value::parseBinary(JsonSource(payload), base64);
		Value conflicts = Value::parseBinary(JsonSource(payload), base64);
		for (Value v: log) revision_map[v.getUInt()] = false;
		for (Value v: conflicts) {
			RevID rev = v.getUInt();
			revision_map[rev] = true;

			if (dbcore.findDoc(h,id,rev,rawdoc,tmp)) {

				std::string_view payload = rawdoc.payload;
				Value log = Value::parseBinary(JsonSource(payload), base64);
				Value conflicts = Value::parseBinary(JsonSource(payload), base64);
				bool chkcommon = true;
				for (Value v: log) {
					RevID rev = v.getUInt();
					if (chkcommon) {
						//make common revisions invicible, to allow future merge
						auto iter = revision_map.find(rev);
						if (iter != revision_map.end()) {
							iter->second = true;
							chkcommon = false;
							continue;
						}
					}
					revision_map[v.getUInt()] = false;
				}
			}
		}
		return true;
	} else {
		return false;
	}
}

void MaintenanceTask::init_task(PEventRouter rt, Handle h, SeqNum s) {
	logInfo("Maintenance monitoring is ACTIVE on db $1 since $2", h, s);
	auto task = [this,rt,h,s,g=Sync(cntd)](bool){
		SeqNum sq;
		if (rt->getLastSeqNum(h,sq)) {
			if (sq != s) {
				logInfo("Maintenance is RUNNING on db $1 since $2", h, s);
				DatabaseCore::RevMap revision_map;

				dbcore.readChanges(h,s,false,[&](const DatabaseCore::ChangeRec &rc) {
					revision_map.clear();
					if (init_rev_map(revision_map,h, rc.docid))
						dbcore.cleanHistory(h,rc.docid, revision_map);
					return true;
				});
			}
			init_task(rt,h,sq);
		} else {
			logInfo("Maintenance monitoring was REMOVED on db $1 since $2", h, s);
		}
	};
	rt->waitForEvent(h,s,1000*86400, task);
}

MaintenanceTask::~MaintenanceTask() {
	if (this->router != nullptr) {
		this->router->removeObserver(this->oh);
		cntd.wait();
	}

}


} /* namespace sofadb */
