/*
 * rpcapi.h
 *
 *  Created on: 9. 2. 2019
 *      Author: ondra
 */

#ifndef SRC_MAIN_RPCAPI_H_
#define SRC_MAIN_RPCAPI_H_

#include <imtjson/rpc.h>
#include "../libsofa/api.h"

namespace sofadb {

class RpcAPI {

	typedef SofaDB::Handle Handle;
	typedef std::shared_ptr<SofaDB> PSofaDB;
public:
	RpcAPI(PSofaDB db);

	void init(json::RpcServer &server);


	void databaseCreate(json::RpcRequest req);
	void databaseDelete(json::RpcRequest req);
	void databaseList(json::RpcRequest req);
	void databaseRename(json::RpcRequest req);
	void databaseChanges(json::RpcRequest req);
	void databaseSetConfig(json::RpcRequest req);
	void databaseStopChanges(json::RpcRequest req);
	void documentGet(json::RpcRequest req);
	void documentPut(json::RpcRequest req);
	void documentChanges(json::RpcRequest req);


protected:
	PSofaDB db;


	bool arg0ToHandle(json::RpcRequest req, DatabaseCore::Handle &h);
	json::Value statusToError(PutStatus st);


	struct NotifyMap {
		std::map<json::String, SofaDB::WaitHandle> notifyMap;
		std::mutex notifyLock;

		bool registerNotify(json::String notifyName, SofaDB::WaitHandle waitHandle);
		bool updateNotify(json::String notifyName, SofaDB::WaitHandle waitHandle);
		SofaDB::WaitHandle stopNotify(json::String notifyName);
	};

	std::shared_ptr<NotifyMap> ntfmap;



};

} /* namespace sofadb */

#endif /* SRC_MAIN_RPCAPI_H_ */
