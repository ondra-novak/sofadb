/*
 * maintenancetask.h
 *
 *  Created on: 20. 2. 2019
 *      Author: ondra
 */

#ifndef MAINTENANCETASK_H_
#define MAINTENANCETASK_H_
#include <libsofa/databasecore.h>

namespace sofadb {

class MaintenanceTask {
public:
	MaintenanceTask(DatabaseCore &dbcore);
	virtual ~MaintenanceTask();
};

} /* namespace sofadb */

#endif /* MAINTENANCETASK_H_ */
