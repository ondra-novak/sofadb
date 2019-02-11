/*
 * systemdbs.h
 *
 *  Created on: 10. 2. 2019
 *      Author: ondra
 */
#include "databasecore.h"



namespace sofadb {

class SystemDBs {
public:

	typedef DatabaseCore::Handle Handle;

	const Handle users;
	const Handle replicator;

	SystemDBs(DatabaseCore &core);

protected:

	static Handle getDBHandle(DatabaseCore &core, const std::string_view &name);
};


}
