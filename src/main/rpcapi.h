/*
 * rpcapi.h
 *
 *  Created on: 9. 2. 2019
 *      Author: ondra
 */

#ifndef SRC_MAIN_RPCAPI_H_
#define SRC_MAIN_RPCAPI_H_

#include <imtjson/rpc.h>
#include "../libsofa/databasecore.h"
#include "../libsofa/docdb.h"

namespace sofadb {

class RpcAPI {
public:
	RpcAPI(DatabaseCore &db):db(db),docdb(db) {}

	void init(json::RpcServer &server);


	void databaseCreate(json::RpcRequest req);
	void databaseDelete(json::RpcRequest req);
	void databaseList(json::RpcRequest req);

protected:
	DatabaseCore &db;
	DocumentDB docdb;

};

} /* namespace sofadb */

#endif /* SRC_MAIN_RPCAPI_H_ */
