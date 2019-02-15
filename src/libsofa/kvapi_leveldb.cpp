/*
 * kvapi_leveldb.cpp
 *
 *  Created on: 8. 2. 2019
 *      Author: ondra
 */

#include <libsofa/kvapi_leveldb_impl.h>
#include "kvapi_leveldb.h"

namespace sofadb {


LevelDBException::LevelDBException(const leveldb::Status &st):status(st) {

}

const char *LevelDBException::what() const noexcept {
	if (msg.empty()) msg = status.ToString();
	return msg.c_str();
}


PKeyValueDatabase leveldb_open(const leveldb::Options& options, const std::string& name) {
	leveldb::DB *db;
	leveldb::Status st = leveldb::DB::Open(options,name,&db);
	if (st.ok()) {
		LevelDBDatabase *d;
		PKeyValueDatabase kvdb = d = new LevelDBDatabase(db, name);
		if (d->isDestroyed()) {
			///this should delete database because it is deleted in destructor
			kvdb = nullptr;
			return leveldb_open(options, name);
		}
		else {
			return kvdb;
		}
	}
	else throw LevelDBException(st);
}



}


