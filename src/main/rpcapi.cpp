/*
 * rpcapi.cpp
 *
 *  Created on: 9. 2. 2019
 *      Author: ondra
 */

#include <imtjson/array.h>
#include <imtjson/object.h>
#include <main/rpcapi.h>

using json::Object;

namespace sofadb {

using namespace json;

void RpcAPI::init(json::RpcServer& server) {
	server.add("Database.create",this,&RpcAPI::databaseCreate);
	server.add("Database.delete",this,&RpcAPI::databaseDelete);
	server.add("Database.list",this,&RpcAPI::databaseList);
}

void RpcAPI::databaseCreate(json::RpcRequest req) {
	static Value args(json::array,{
			Object("name","string")
				  ("auth","string")
	});
	if (!req.checkArgs(args)) return req.setArgError();
	Value a = req.getArgs();
	String name = a[0]["name"].toString();
	auto h = db.create(name.str());
	if (h == db.invalid_handle) return req.setError(400, "Invalid database name",name);
	req.setResult(true);
}

void RpcAPI::databaseDelete(json::RpcRequest req) {
	static Value args(json::array,{
			Object("name","string")
				  ("auth","string")
	});
	if (!req.checkArgs(args)) return req.setArgError();
	Value a = req.getArgs();
	String name = a[0]["name"].toString();
	auto h = db.getHandle(name.str());
	if (h == db.invalid_handle) req.setError(404,"not_found", name);
	db.erase(h);
	req.setResult(true);
}

void RpcAPI::databaseList(json::RpcRequest req) {
	static Value args(json::array,{
			Object("auth","string")
	});
	if (!req.checkArgs(args)) return req.setArgError();
	Value a = req.getArgs();
	Array out;
	db.list([&](std::string_view name, DatabaseCore::Handle h){
		Object nfo;
		nfo.set("name",StrViewA(name))
			   ("id",h)
			   ("config",Object("max_logsize",db.getMaxLogSize(h))
					   	   	   ("max_age",db.getMaxAge(h)));

		out.push_back(nfo);
		return true;
	});
	req.setResult(out);

}

} /* namespace sofadb */

