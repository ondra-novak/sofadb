/*
 * systemdbs.cpp
 *
 *  Created on: 10. 2. 2019
 *      Author: ondra
 */

#include "systemdbs.h"

namespace sofadb {


SystemDBs::Handle SystemDBs::getDBHandle(DatabaseCore &core, const std::string_view &name) {
	Handle h = core.getHandle(name);
	if (h == core.invalid_handle) h = core.create(name);
	return h;
}

SystemDBs::SystemDBs(DatabaseCore &core)
	:users(getDBHandle(core,"_users"))
	,replicator(getDBHandle(core,"_replicator")) {
}

}


