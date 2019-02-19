/*
 * debugapi.h
 *
 *  Created on: 19. 2. 2019
 *      Author: ondra
 */

#ifndef SRC_MAIN_DEBUGAPI_H_
#define SRC_MAIN_DEBUGAPI_H_
#include <imtjson/rpc.h>
#include <libsofa/kvapi.h>

namespace sofadb {

class DebugAPI {
public:
	DebugAPI(PKeyValueDatabase kvdb);
	virtual ~DebugAPI();

	void init(json::RpcServer &server);


	void rpcDump(json::RpcRequest req);
	void rpcErase(json::RpcRequest req);
	void rpcPut(json::RpcRequest req);

protected:
	PKeyValueDatabase kvdb;
};

} /* namespace sofadb */

#endif /* SRC_MAIN_DEBUGAPI_H_ */
