/*
 * debugapi.cpp
 *
 *  Created on: 19. 2. 2019
 *      Author: ondra
 */

#include <imtjson/array.h>
#include <imtjson/binary.h>
#include <imtjson/string.h>
#include "debugapi.h"

namespace sofadb {

using namespace json;

DebugAPI::DebugAPI(PKeyValueDatabase kvdb):kvdb(kvdb) {
}

DebugAPI::~DebugAPI() {
	// TODO Auto-generated destructor stub
}

void DebugAPI::init(RpcServer& server) {

	server.add("Debug.dump",this,&DebugAPI::rpcDump);
	server.add("Debug.erase",this,&DebugAPI::rpcErase);
	server.add("Debug.put",this,&DebugAPI::rpcPut);
	server.add("Debug.setCtx",this,&DebugAPI::rpcSetCtx);
	server.add("Debug.getCtx",this,&DebugAPI::rpcGetCtx);
}

void DebugAPI::rpcDump(json::RpcRequest req) {
	static Value argformat = {"string",{"number","undefined"},{"number","undefined"}};

	if (!req.checkArgs(argformat)) return req.setArgError();

	Value args = req.getArgs();
	String key = args[0].toString();
	std::uint64_t limit = args[1].getUInt();
	if (limit == 0) limit =100;
	std::uint64_t offset = args[2].getUInt()*limit;

	Iterator iter (kvdb->findRange(StrViewA(key),false));
	Object result;
	while (limit && iter.getNext()) {
		if (offset > 0) {
			--offset;
		} else {
			result.set(utf8encoding->encodeBinaryValue(BinaryView(StrViewA(iter->first))).getString(),
					   utf8encoding->encodeBinaryValue(BinaryView(StrViewA(iter->second))));
			limit--;
		}
	}
	req.setResult(result);
}

void DebugAPI::rpcErase(json::RpcRequest req) {
	static Value argformat = {"string",{"undefined","boolean"}};
	if (!req.checkArgs(argformat)) return req.setArgError();
	Value args = req.getArgs();
	StrViewA key = args[0].getString();

	auto chset = kvdb->createChangeset();
	if (args[1].getBool()) {
		chset->erasePrefix(key);
	} else {
		chset->erase(key);
	}
	chset->commit();
	req.setResult(true);
}

void DebugAPI::rpcPut(json::RpcRequest req) {
	static Value argformat = {"string","string"};
	if (!req.checkArgs(argformat)) return req.setArgError();
	Value args = req.getArgs();

	auto chset = kvdb->createChangeset();
	chset->put(args[0].getString(),args[1].getString());
	chset->commit();
	req.setResult(true);
}

void DebugAPI::rpcSetCtx(json::RpcRequest req) {
	static Value argformat = {"string", "any"};
	if (!req.checkArgs(argformat)) return req.setArgError();
	Value args = req.getArgs();
	StrViewA key = args[0].getString();
	Value v = args[1];
	req.getConnContext()->store(key, v);
	req.setResult(true);

}
void DebugAPI::rpcGetCtx(json::RpcRequest req) {
	static Value argformat = {"string"};
	if (!req.checkArgs(argformat)) return req.setArgError();
	Value args = req.getArgs();
	StrViewA key = args[0].getString();
	Value v = req.getConnContext()->retrieve(key);
	if (v.defined())
		req.setResult(v);
	else
		req.setError(404,"not_found");

}


} /* namespace sofadb */
