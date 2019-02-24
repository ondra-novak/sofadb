/*
 * maintenancetask.cpp
 *
 *  Created on: 20. 2. 2019
 *      Author: ondra
 */

#include <shared/logOutput.h>
#include "maintenancetask.h"

using ondra_shared::logInfo;

namespace sofadb {


using ondra_shared::logInfo;

MaintenanceTask::MaintenanceTask(DatabaseCore& dbcore):dbcore(dbcore) {
}

void MaintenanceTask::init(PEventRouter router) {

	Sync _(lock);

	if (this->router != nullptr) {
		this->router->removeObserver(this->oh);
	}
	this->router = router;
	std::vector<std::pair<Handle,SeqNum> > dblist;
	router->listDBs([&](auto &&item) {dblist.push_back(item);return true;});
	for (auto &&item:dblist) {
		Handle h = item.first;
		SeqNum n = item.second;
		init_task(dbcore,router,h,n);
	}


	this->oh = this->router->registerObserver([=](DatabaseCore::ObserverEvent ev, Handle h, SeqNum sq){
		if (ev == DatabaseCore::event_create) {
			Sync _(lock);
			init_task(dbcore,router,h,sq);
		}
		return true;
	});
}

void MaintenanceTask::init_task(DatabaseCore &dbcore, PEventRouter rt, Handle h, SeqNum s) {
	logInfo("Maintenance monitoring is ACTIVE on db $1 since $2", h, s);
	auto task = [&dbcore,rt,h,s](bool){
		SeqNum sq;
		if (rt->getLastSeqNum(h,sq)) {
			if (sq != s) {
				logInfo("Maintenance is RUNNING on db $1 since $2", h, s);
				dbcore.readChanges(h,s,false,[&](const std::string_view &docId, SeqNum) {
					dbcore.cleanHistory(h,docId);
					return true;
				});
			}
			init_task(dbcore, rt,h,sq);
		} else {
			logInfo("Maintenance monitoring was REMOVED on db $1 since $2", h, s);
		}
	};
	EventRouter::WaitHandle wh = rt->waitForEvent(h,s,1000*86400, task);
	if (wh == 0) {
		task(true);
	}

}

MaintenanceTask::~MaintenanceTask() {
	if (this->router != nullptr) {
		this->router->removeObserver(this->oh);
	}
}


} /* namespace sofadb */
