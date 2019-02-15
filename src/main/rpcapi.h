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
	void databaseRename(json::RpcRequest req);
	void documentGet(json::RpcRequest req);
	void documentPut(json::RpcRequest req);
	void documentList(json::RpcRequest req);

protected:
	DatabaseCore &db;
	DocumentDB docdb;

	bool arg0ToHandle(json::RpcRequest req, DatabaseCore::Handle &h);
	json::Value statusToError(PutStatus st);

};

} /* namespace sofadb */

#endif /* SRC_MAIN_RPCAPI_H_ */
