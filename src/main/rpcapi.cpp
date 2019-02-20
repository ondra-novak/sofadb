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
#include <shared/shared_function.h>

using json::Object;

namespace sofadb {

using namespace json;

RpcAPI::RpcAPI(PSofaDB db):db(db),ntfmap(std::make_shared<NotifyMap>()) {}

void RpcAPI::init(json::RpcServer& server) {
	server.add("DB.create",this,&RpcAPI::databaseCreate);
	server.add("DB.delete",this,&RpcAPI::databaseDelete);
	server.add("DB.list",this,&RpcAPI::databaseList);
	server.add("DB.setConfig",this,&RpcAPI::databaseSetConfig);
	server.add("DB.rename",this,&RpcAPI::databaseRename);
	server.add("DB.changes",this,&RpcAPI::databaseChanges);
	server.add("DB.stopChanges",this,&RpcAPI::databaseStopChanges);
	server.add("Doc.put",this,&RpcAPI::documentPut);
	server.add("Doc.get",this,&RpcAPI::documentGet);
}

void RpcAPI::databaseCreate(json::RpcRequest req) {
	static Value args(json::array,{"string"});

	if (!req.checkArgs(args)) return req.setArgError();
	Value a = req.getArgs();
	String name = a[0].toString();
	auto h = db->createDB(name.str());
	if (h == db->invalid_handle) return req.setError(400, "Invalid database name",name);
	req.setResult(true);
}

void RpcAPI::databaseDelete(json::RpcRequest req) {
	static Value args(json::array,{{"string","integer"}});
	if (!req.checkArgs(args)) return req.setArgError();
	Handle h;
	if (!arg0ToHandle(req,h)) return;
	db->deleteDB(h);
	req.setResult(true);
}

json::Value dbconfig2json(const DatabaseCore::DBConfig &cfg);
void json2dbconfig(json::Value data, DatabaseCore::DBConfig &cfg);

void RpcAPI::databaseSetConfig(json::RpcRequest req) {
	static Value form({{"string","integer"}, Object("%","any")});
	if (!req.checkArgs(form)) return req.setArgError();
	Value args = req.getArgs();
	Handle h;
	if (!arg0ToHandle(req,h)) return;
	DatabaseCore::DBConfig cfg;
	db->getDBCore().getConfig(h, cfg);
	json2dbconfig(args[1],cfg);
	db->getDBCore().setConfig(h, cfg);
	req.setResult(true);

}


void RpcAPI::databaseList(json::RpcRequest req) {
	static Value args(json::array,{});
	if (!req.checkArgs(args)) return req.setArgError();
	Array out;
	db->listDB([&](std::string_view name, DatabaseCore::Handle h){
		Object nfo;
		DatabaseCore::DBConfig cfg;
		db->getDBCore().getConfig(h,cfg);
		nfo.set("name",StrViewA(name))
			   ("id",h)
			   ("config",dbconfig2json(cfg));

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
	if (!db->renameDB(h,to)) return req.setError(409,"conflict");
	req.setResult(true);
}

bool RpcAPI::arg0ToHandle(json::RpcRequest req, DatabaseCore::Handle &h) {
	Value arg0 = req.getArgs()[0];
	if (arg0.type() == json::number && arg0.flags() & json::numberUnsignedInteger) {
		h = arg0.getUInt();
		return true;
	}
	if (arg0.type() == json::string) {
		auto hh = db->getDB(arg0.getString());
		if (hh != db->invalid_handle) {
			h = hh;
			return true;
		}
	}
	req.setError(404,"not_found", arg0);
	return false;
}

static std::size_t getLimit(const Value &v)  {
	Value x = v["limit"];
	if (x.defined()) return v.getUInt();
	else return static_cast<std::size_t>(-1);
}

static std::size_t getOffset(const Value &v)  {
	Value x = v["skip"];
	if (x.defined()) return v.getUInt();
	else return 0;
}

static OutputFormat getOutputFormat(OutputFormat lf, Value v) {
	Value log = v["log"];
	Value del = v["deleted"];
	Value data = v["data"];

	if (log.defined()) {
		if (log.getBool()) lf = lf | OutputFormat::log;
		else lf =lf - OutputFormat::log;
	}
	if (del.defined()) {
		if (del.getBool()) lf = lf | OutputFormat::deleted;
		else lf = lf - OutputFormat::deleted;
	}
	if (data.defined()){
		if (data.getBool()) lf = lf | OutputFormat::data;
		else lf = lf - OutputFormat::data;
	}
	return lf;

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
			res = db->get(h, v.getString(), f);
		} else if (v.type() == json::object) {
			Value id = v["id"];
			Value prefix=v["prefix"];
			Value start_key=v["start_key"];
			Value end_key=v["end_key"];
			OutputFormat lf = getOutputFormat(f, v);
			Value rev = v["rev"];

			if (id.defined()) {
				if (rev.defined()) {
					res = db->get(h, id.getString(), rev.getString(), lf);
				} else {
					res = db->get(h, id.getString(), lf);
				}
			} else if (prefix.defined() || start_key.defined() || end_key.defined()) {
				std::size_t limit = getLimit(v);
				Array l;
				if (limit) {
					std::size_t offset = getOffset(v);
					auto cb =  [&](const Value &v) {
						if (offset) {
							--offset;return true;
						}
						l.push_back(v);
						return 	--limit > 0;
					};
					if (prefix.defined()) {
						bool rev = v["descending"].getBool();
						db->allDocs(h, lf, prefix.getString(), rev, std::move(cb));
					} else {
						db->allDocs(h, lf, start_key.getString(), end_key.getString(), std::move(cb));
					}
				}
				res = l;
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

typedef ondra_shared::shared_function<void(bool)> SharedObserver;


void RpcAPI::databaseChanges(json::RpcRequest req) {

	Value args = req.getArgs();

	DatabaseCore::Handle h;
	if (!arg0ToHandle(req,h)) return;

	Value cfg = args[1];
	OutputFormat fmt = getOutputFormat(OutputFormat::metadata_only, cfg);
	Value vntfname = cfg["notify"];
	std::size_t timeout = cfg["timeout"].getUInt();
	std::size_t since = cfg["since"].getUInt();
	std::size_t offset = getOffset(cfg);
	std::size_t limit = getLimit(cfg);
	bool reversed = cfg["descending"].getBool();
	std::chrono::time_point<std::chrono::steady_clock> now = std::chrono::steady_clock::now();
	auto abstm = now + std::chrono::milliseconds(timeout);
	PSofaDB rdb = db;
	auto nmap = ntfmap;
	Value filter = cfg["filter"];
	DocFilter flt = createFilter(filter);

	String ntfname;

	if (vntfname.defined()) {
		ntfname = vntfname.toString();
		if (!nmap->registerNotify(ntfname, 0)) {
			req.setError(409,"conflict", ntfname);
			return;
		}

		SharedObserver observer = [rdb,nmap,ntfname,h,since,reversed,fmt,flt,req,offset,limit,abstm](SharedObserver self, bool not_timeout) mutable {
			if (not_timeout) {
				bool failed = false;
				since = rdb->readChanges(h, since, reversed, fmt, std::move(flt), [&](const Value &x) {
					if (offset) {
						offset--;
						return true;
					}
					else {
						if (!req.sendNotify(ntfname,x)) {
							failed = true;
							return false;
						}
						return --limit>0;
					}
				});

				std::chrono::time_point<std::chrono::steady_clock> now = std::chrono::steady_clock::now();
				if (!failed && now < abstm) {
					std::size_t tm = std::chrono::duration_cast<std::chrono::milliseconds>(abstm-now).count();
					SofaDB::WaitHandle wh = rdb->waitForChanges(h, since, tm, self);
					if (!wh) {
						self(true);
					} else {
						if (!nmap->updateNotify(ntfname, wh)) {
							if (rdb->cancelWaitForChanges(wh)) {
								req.setError(410,"canceled");
							}
						}
					}
				} else {
					self(false);
				}
			} else  {
				req.setResult(Object("seq",since));
				nmap->stopNotify(ntfname);

			}
		};

		observer(true);
	} else {
		SharedObserver observer = [rdb,h,since,reversed,fmt,flt,req,offset,limit,abstm](SharedObserver self, bool not_timeout) mutable {
			if (not_timeout) {
				Array res;
				since = rdb->readChanges(h, since, reversed, fmt, std::move(flt), [&](const Value &x) {
					if (offset) {
						offset--;
						return true;
					}
					else {
						res.push_back(x);
						return --limit>0;
					}
				});
				if (res.empty()) {
					std::chrono::time_point<std::chrono::steady_clock> now = std::chrono::steady_clock::now();
					if (now < abstm) {
						std::size_t tm = std::chrono::duration_cast<std::chrono::milliseconds>(abstm-now).count();
						SofaDB::WaitHandle wh = rdb->waitForChanges(h, since, tm, self);
						if (!wh) {
							self(true);
						}
					} else {
						self(false);
					}
				} else {
					req.setResult(res);
				}
			} else  {
				req.setResult(json::array);
			}
		};
		observer(true);
	}
}

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
		PutStatus st = db->put(h,doc,newrev);
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

bool RpcAPI::NotifyMap::registerNotify(String notifyName, SofaDB::WaitHandle waitHandle) {
	std::lock_guard<std::mutex> _(notifyLock);
	auto iter = notifyMap.find(notifyName);
	if (iter != notifyMap.end()) return false;
	notifyMap.insert(std::pair(notifyName,waitHandle));
	return true;

}

bool RpcAPI::NotifyMap::updateNotify(json::String notifyName,SofaDB::WaitHandle waitHandle) {
	std::lock_guard<std::mutex> _(notifyLock);
	auto iter = notifyMap.find(notifyName);
	if (iter == notifyMap.end()) return false;
	iter->second = waitHandle;
	return true;
}

void RpcAPI::databaseStopChanges(json::RpcRequest req) {
	String id = req.getArgs()[0].toString();
	SofaDB::WaitHandle wh = ntfmap->stopNotify(id);
	if (wh == 0) req.setError(404,"not_found",id);
	db->cancelWaitForChanges(wh);
	req.setResult(true);
}

SofaDB::WaitHandle RpcAPI::NotifyMap::stopNotify(json::String notifyName) {
	std::lock_guard<std::mutex> _(notifyLock);
	auto iter = notifyMap.find(notifyName);
	if (iter == notifyMap.end()) return 0;
	SofaDB::WaitHandle wh = iter->second;
	notifyMap.erase(iter);
	return wh;
}
} /* namespace sofadb */

