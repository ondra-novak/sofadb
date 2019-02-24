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
	,mtask(dbcore)
{
	dbcore.setObserver(eventRouter->createObserver());
	mtask.init(eventRouter);
}

SofaDB::SofaDB(PKeyValueDatabase kvdatabase, Worker worker)
	:dbcore(kvdatabase)
	,docdb(dbcore)
	,eventRouter(new EventRouter(worker))
	,mtask(dbcore)

{
	dbcore.setObserver(eventRouter->createObserver());
	mtask.init(eventRouter);
}

SofaDB::~SofaDB() {
	eventRouter->stop();
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
	return docdb.listDocs(db, std::string_view(),false,outputFormat,std::move(cb));
}

bool SofaDB::allDocs(Handle db, OutputFormat outputFormat,
		const std::string_view& prefix, ResultCB&& cb) {
	return docdb.listDocs(db, prefix,false,outputFormat,std::move(cb));
}

bool SofaDB::allDocs(Handle db, OutputFormat outputFormat,
		const std::string_view& prefix, bool reversed, ResultCB&& cb) {
	return docdb.listDocs(db, prefix,reversed,outputFormat,std::move(cb));
}

bool SofaDB::allDocs(Handle db, OutputFormat outputFormat,
		const std::string_view& start_key, const std::string_view& end_key,
		ResultCB&& cb) {
	return docdb.listDocs(db, start_key, end_key,outputFormat,std::move(cb));
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

SofaDB::ObserverHandle SofaDB::registerObserver(GlobalObserver&& observer) {
	return eventRouter->registerObserver(std::move(observer));
}

bool SofaDB::removeObserver(ObserverHandle handle) {
	return eventRouter->removeObserver(handle);
}

DocumentDB& SofaDB::getDocDB() {
	return docdb;
}

DatabaseCore& SofaDB::getDBCore() {
	return dbcore;
}

SeqNum SofaDB::readChanges(Handle h, SeqNum since,bool reversed, OutputFormat format, ResultCB&& callback) {
	return docdb.readChanges(h, since, reversed, format, std::move(callback));
}
SeqNum SofaDB::readChanges(Handle h, SeqNum since, bool reversed, OutputFormat format, DocFilter &&flt, ResultCB &&callback) {
	return docdb.readChanges(h, since, reversed, format, std::move(flt), std::move(callback));
}

SofaDB::WaitHandle SofaDB::waitForChanges(Handle h, SeqNum since, std::size_t timeout_ms, Observer&& observer) {
	return eventRouter->waitForEvent(h, since,timeout_ms,std::move(observer));
}

bool SofaDB::cancelWaitForChanges(WaitHandle wh) {
	return eventRouter->cancelWait(wh);
}

PEventRouter SofaDB::getEventRouter() {
	return eventRouter;
}
}

