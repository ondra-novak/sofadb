/*
 * rpcapi.cpp
 *
 *  Created on: 9. 2. 2019
 *      Author: ondra
 */

#include <imtjson/array.h>
#include <imtjson/object.h>
#include <main/rpcapi.h>
#include <shared/logOutput.h>

using json::Object;

namespace sofadb {

using namespace json;

void RpcAPI::init(json::RpcServer& server) {
	server.add("DB.create",this,&RpcAPI::databaseCreate);
	server.add("DB.delete",this,&RpcAPI::databaseDelete);
	server.add("DB.list",this,&RpcAPI::databaseList);
	server.add("DB.rename",this,&RpcAPI::databaseRename);
	server.add("Doc.put",this,&RpcAPI::documentPut);
	server.add("Doc.get",this,&RpcAPI::documentGet);
	server.add("Doc.list",this,&RpcAPI::documentList);
}

void RpcAPI::databaseCreate(json::RpcRequest req) {
	static Value args(json::array,{"string"});

	if (!req.checkArgs(args)) return req.setArgError();
	Value a = req.getArgs();
	String name = a[0].toString();
	auto h = db.create(name.str());
	if (h == db.invalid_handle) return req.setError(400, "Invalid database name",name);
	req.setResult(true);
}

void RpcAPI::databaseDelete(json::RpcRequest req) {
	static Value args(json::array,{"string"});
	if (!req.checkArgs(args)) return req.setArgError();
	Value a = req.getArgs();
	String name = a[0].toString();
	auto h = db.getHandle(name.str());
	if (h == db.invalid_handle) req.setError(404,"not_found", name);
	db.erase(h);
	req.setResult(true);
}

void RpcAPI::databaseList(json::RpcRequest req) {
	static Value args(json::array,{});
	if (!req.checkArgs(args)) return req.setArgError();
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

void RpcAPI::databaseRename(json::RpcRequest req) {
	static Value aform = {{"string","number"},"string"};
	if (!req.checkArgs(aform)) return req.setArgError();
	StrViewA to = req.getArgs()[1].getString();
	DatabaseCore::Handle h;
	if (!arg0ToHandle(req,h)) return;
	if (!db.rename(h,to)) return req.setError(409,"conflict");
	req.setResult(true);
}

bool RpcAPI::arg0ToHandle(json::RpcRequest req, DatabaseCore::Handle &h) {
	Value arg0 = req.getArgs()[0];
	if (arg0.type() == json::number && arg0.flags() & json::numberUnsignedInteger) {
		h = arg0.getUInt();
		return true;
	}
	if (arg0.type() == json::string) {
		auto hh = db.getHandle(arg0.getString());
		if (hh != db.invalid_handle) {
			h = hh;
			return true;
		}
	}
	req.setError(404,"not_found", arg0);
	return false;
}

void RpcAPI::documentGet(json::RpcRequest req) {
	static Value aform = {{"string","number"},
			{"string",Object("id","string")("rev","string")},
			{"undefined",Object("log","boolean")
							   ("deleted","boolean")}};
	if (!req.checkArgs(aform)) return req.setArgError();
	OutputFormat f = OutputFormat::data;

	DatabaseCore::Handle h;
	if (!arg0ToHandle(req,h)) return;

	Value doc = req.getArgs()[1];

	Value opts = req.getArgs()[2];
	if (opts["log"].getBool()) f = f | OutputFormat::log;
	if (opts["deleted"].getBool()) f = f | OutputFormat::deleted;

	Value out;
	if (doc.type() == json::string) {
		out = docdb.get(h,doc.getString(), f);
	} else if (doc.type() == json::object) {
		Value id = doc["id"];
		Value rev = doc["rev"];
		out = docdb.get(h,doc.getString(),rev.getString(), f);
	}

	if (out.defined()) {
		req.setResult(out);
	} else {
		req.setError(404,"not_found");
	}



}

Value RpcAPI::statusToError(PutStatus st) {
	unsigned int code;
	String message;
	switch (st) {
	case PutStatus::conflict: code = 409; message = "conflict";break;
	case PutStatus::db_not_found: code = 404; message = "db_not_found";break;
	case PutStatus::error_log_is_mandatory: code = 400; message = "'log' is mandatory";break;
	default:
		code=500; message = "unknown_error";
		ondra_shared::logError("Unknown status $1", static_cast<unsigned int>(st));
		break;
	}

	return RpcServer::defaultFormatError(code, message, Value());
}

void RpcAPI::documentPut(json::RpcRequest req) {
	static Value aform = {
			{"string","number"},
			Object("id","string")
				  ("rev",{"undefined","string"})
				  ("conflicts",{"undefined",{{},"string"}})
				  ("data","any")
				  ("log",{"undefined",{{},"string"}}),
		{"undefined",Object("replication","boolean")}};

	if (!req.checkArgs(aform)) return req.setArgError();

	DatabaseCore::Handle h;
	if (!arg0ToHandle(req,h)) return;

	bool replication = req.getArgs()[2]["replication"].getBool();

	Value doc = req.getArgs()[1];

	PutStatus st;
	String newrev;

	if (replication) {
		st = docdb.replicator_put(h,doc);
		if (st == PutStatus::stored) return req.setResult(true);
	} else {
		st = docdb.client_put(h,doc, newrev);
		if (st == PutStatus::stored) return req.setResult(newrev);
	}

	req.setError(statusToError(st));

}

void RpcAPI::documentList(json::RpcRequest req) {
}

} /* namespace sofadb */

