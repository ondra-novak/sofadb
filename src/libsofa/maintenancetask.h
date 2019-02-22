/*
 * maintenancetask.h
 *
 *  Created on: 20. 2. 2019
 *      Author: ondra
 */

#ifndef MAINTENANCETASK_H_
#define MAINTENANCETASK_H_
#include "databasecore.h"
#include "eventrouter.h"

namespace sofadb {

class MaintenanceTask {
public:
	using Handle = DatabaseCore::Handle;

	MaintenanceTask(DatabaseCore &dbcore);

	void init(PEventRouter router);

	virtual ~MaintenanceTask();
protected:

	static void init_task(DatabaseCore &dbcore, PEventRouter rt, Handle h, SeqNum s);

	std::mutex lock;
	typedef std::unique_lock<std::mutex> Sync;
	PEventRouter router;
	EventRouter::ObserverHandle oh;



	DatabaseCore &dbcore;
};

} /* namespace sofadb */

#endif /* MAINTENANCETASK_H_ */
