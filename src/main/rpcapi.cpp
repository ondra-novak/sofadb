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
	auto h = db.createDB(name.str());
	if (h == db.invalid_handle) return req.setError(400, "Invalid database name",name);
	req.setResult(true);
}

void RpcAPI::databaseDelete(json::RpcRequest req) {
	static Value args(json::array,{{"string","unsigned"}});
	if (!req.checkArgs(args)) return req.setArgError();
	Handle h;
	if (!arg0ToHandle(req,h)) return;
	db.deleteDB(h);
	req.setResult(true);
}

void RpcAPI::databaseList(json::RpcRequest req) {
	static Value args(json::array,{});
	if (!req.checkArgs(args)) return req.setArgError();
	Array out;
	db.listDB([&](std::string_view name, DatabaseCore::Handle h){
		Object nfo;
		nfo.set("name",StrViewA(name))
			   ("id",h)
			   ("config",Object("max_logsize",db.getDBCore().getMaxLogSize(h))
					   	   	   ("max_age",db.getDBCore().getMaxAge(h)));

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
	if (!db.renameDB(h,to)) return req.setError(409,"conflict");
	req.setResult(true);
}

bool RpcAPI::arg0ToHandle(json::RpcRequest req, DatabaseCore::Handle &h) {
	Value arg0 = req.getArgs()[0];
	if (arg0.type() == json::number && arg0.flags() & json::numberUnsignedInteger) {
		h = arg0.getUInt();
		return true;
	}
	if (arg0.type() == json::string) {
		auto hh = db.getDB(arg0.getString());
		if (hh != db.invalid_handle) {
			h = hh;
			return true;
		}
	}
	req.setError(404,"not_found", arg0);
	return false;
}

void RpcAPI::documentGet(json::RpcRequest req) {

	Value args = req.getArgs();

	std::uintptr_t cnt = args.size();

	DatabaseCore::Handle h;
	if (!arg0ToHandle(req,h)) return;

	OutputFormat f = OutputFormat::data;

	Array result;
	for (std::uintptr_t i = 1; i < cnt ; i++) {
		Value v = args[i];
		Value res;
		if (v.type() == json::string) {
			res = db.get(h, v.getString(), f);
		} else if (v.type() == json::object) {
			Value id = v["id"];
			OutputFormat lf = f;
			Value rev = v["rev"];
			Value log = v["log"];
			Value del = v["deleted"];

			if (log.defined()) {
				if (log.getBool()) lf = lf | OutputFormat::log;
				else lf =lf - OutputFormat::log;
			}
			if (del.defined()) {
				if (del.getBool()) lf = lf | OutputFormat::deleted;
				else lf = lf - OutputFormat::deleted;
			}
			if (id.defined()) {
				if (rev.defined()) {
					res = db.get(h, id.getString(), rev.getString(), lf);
				} else {
					res = db.get(h, id.getString(), lf);
				}
			} else {
				f = lf;
			}
		}
		if (res.defined()) {
			if (res.isNull()) {
				Object errobj(RpcServer::defaultFormatError(404, "not_found",Value()));
				errobj.merge(v);
				errobj.set("error",true);
				result.push_back(errobj);
			} else {
				result.push_back(res);
			}
		}

	}
	req.setResult(result);
}


PutStatus2Error status2error[13]={
		{PutStatus::stored, 0, "stored"},
		{PutStatus::merged, 0, "merged"},
		{PutStatus::conflict, 409, "conflict"},
		{PutStatus::db_not_found, 444, "databae_not_found"},
		{PutStatus::error_id_must_be_string, 451,"'id' must be string"},
		{PutStatus::error_rev_must_be_string,452,"'rev' must be string"},
		{PutStatus::error_conflicts_must_be_array,453,"'conflicts' must be array"},
		{PutStatus::error_conflict_must_be_string,454,"'conflict' must be string"},
		{PutStatus::error_deleted_must_be_bool,455,"'deleted' must be boolean"},
		{PutStatus::error_timestamp_must_be_number,456,"'timestamp' must be number"},
		{PutStatus::error_data_is_manadatory,457,"'data' is mandatory"},
		{PutStatus::error_log_is_mandatory,458,"'log' is mandatory"},
		{PutStatus::error_log_item_must_be_string,459,"'log' item must be string"}
};


Value RpcAPI::statusToError(PutStatus st) {

	for (auto &&c: status2error) {
		if (c.status == st) return RpcServer::defaultFormatError(c.code, StrViewA(c.msg), Value());
	}
	return RpcServer::defaultFormatError(518, "Unknown error", Value((int)st));
}

void RpcAPI::documentPut(json::RpcRequest req) {

	Value args = req.getArgs();

	std::uintptr_t cnt = args.size();

	DatabaseCore::Handle h;
	if (!arg0ToHandle(req,h)) return;


	Array result;
	for (std::uintptr_t i = 1; i < cnt ; i++) {

		Value doc = args[i];
		String newrev;
		PutStatus st = db.put(h,doc,newrev);
		if (st == PutStatus::stored) {
			result.push_back(Object("id",doc["id"])
								   ("rev",newrev));

		} else {
			Object err(statusToError(st));
			err.set("error",true);
			err.set("id", doc["id"]);
			result.push_back(err);
		}

	}
	req.setResult(result);
}

void RpcAPI::documentList(json::RpcRequest req) {
}

} /* namespace sofadb */

