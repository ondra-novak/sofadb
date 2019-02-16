/*
 * api.cpp
 *
 *  Created on: 13. 2. 2019
 *      Author: ondra
 */

#include <imtjson/object.h>
#include <imtjson/string.h>
#include "api.h"


namespace sofadb {

using namespace json;

SofaDB::SofaDB(PKeyValueDatabase kvdatabase)
	:dbcore(kvdatabase)
	,docdb(dbcore)
	,eventRouter(new EventRouter(Worker::create(1)))
{
	dbcore.setObserver(eventRouter->createObserver());
}

SofaDB::SofaDB(PKeyValueDatabase kvdatabase, Worker worker)
	:dbcore(kvdatabase)
	,docdb(dbcore)
	,eventRouter(new EventRouter(worker))

{
	dbcore.setObserver(eventRouter->createObserver());
}


SofaDB::Handle SofaDB::createDB(const std::string_view& name) {
	return dbcore.create(name);

}

SofaDB::Handle SofaDB::getDB(const std::string_view& name) {
	return dbcore.getHandle(name);
}

bool SofaDB::deleteDB(Handle h) {
	return dbcore.erase(h);
}

bool SofaDB::renameDB(Handle h, const std::string_view& name) {
	return dbcore.rename(h,name);
}

bool SofaDB::allDocs(Handle db, OutputFormat outputFormat, ResultCB&& cb) {
}

bool SofaDB::allDocs(Handle db, OutputFormat outputFormat,
		const std::string_view& prefix, ResultCB&& cb) {
}

bool SofaDB::allDocs(Handle db, OutputFormat outputFormat,
		const std::string_view& prefix, bool reversed, ResultCB&& cb) {
}

bool SofaDB::allDocs(Handle db, OutputFormat outputFormat,
		const std::string_view& start_key, const std::string_view& end_key,
		ResultCB&& cb) {
}

PutStatus SofaDB::put(Handle db, const json::Value& doc, json::String &newrev) {
	auto st = docdb.client_put(db,doc, newrev);
	return st;
}

PutStatus SofaDB::replicatorPut(Handle db, const json::Value& doc,	bool history) {
	if (history) {
		return docdb.replicator_put_history(db, doc);
	} else {
		return docdb.replicator_put(db, doc);
	}
}

json::Value SofaDB::get(Handle h, const std::string_view& id, OutputFormat format) {
	return docdb.get(h,id,format);
}

json::Value SofaDB::get(Handle h, const std::string_view& id, const std::string_view& rev, OutputFormat format) {
	return docdb.get(h,id,rev,format);
}

PutStatus SofaDB::erase(Handle h, const std::string_view& docid, const std::string_view& revid) {
	json::Value v = Object("id",StrViewA(docid))
						("rev",StrViewA(revid))
						("deleted",true)
						("value",nullptr);
	String s;
	return put(h, v, s);
}

void SofaDB::purge(Handle h, const std::string_view& docid,	const std::string_view& revid) {
	//docdb.purge(docid, revid);
}

void SofaDB::purge(Handle h, const std::string_view& docid) {
	//docdb.purge(docid);
}

SofaDB::WaitHandle SofaDB::monitorChanges(Handle h, SeqNum since, OutputFormat outputFormat, std::size_t timeout, ResultCB  callback) {
	//eventRouter->waitForEvent(db,since,[])
}

bool SofaDB::cancelMonitor(WaitHandle wh) {
}

SofaDB::ObserverHandle SofaDB::registerObserver(GlobalObserver&& observer) {
}

bool SofaDB::removeObserver(ObserverHandle handle) {
}

DatabaseCore& SofaDB::getDatabaseCore() {
}

DocumentDB& SofaDB::getDocumentDB() {
}

DatabaseCore& SofaDB::getDBCore() {
	return dbcore;
}

EventRouter& SofaDB::getEventRouter() {
}

}

