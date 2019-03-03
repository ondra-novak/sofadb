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

	void init_task(PEventRouter rt, Handle h, SeqNum s);

	using Sync = ondra_shared::CountdownGuard;
	ondra_shared::Countdown cntd;
	PEventRouter router;
	EventRouter::ObserverHandle oh;

	bool init_rev_map(DatabaseCore::RevMap &revision_map, Handle h, const std::string_view &id);


	DatabaseCore &dbcore;
};

} /* namespace sofadb */

#endif /* MAINTENANCETASK_H_ */
