/*
 * kvapi_leveldb_impl.h
 *
 *  Created on: 8. 2. 2019
 *      Author: ondra
 */

#ifndef SRC_LIBSOFA_KVAPI_LEVELDB_IMPL_H_
#define SRC_LIBSOFA_KVAPI_LEVELDB_IMPL_H_

#include <memory>
#include <leveldb/db.h>
#include <leveldb/write_batch.h>
#include "kvapi.h"

namespace sofadb {

///Only allows to iterate one direction of a range.
class LevelDBIteratorBase: public AbstractIterator {
public:
	LevelDBIteratorBase(leveldb::Iterator *iter);
	virtual bool getNext(KeyValue &row);

protected:
	std::unique_ptr<leveldb::Iterator> iter;
	bool (LevelDBIteratorBase::*get_next)(KeyValue &row);
	bool first(KeyValue &row);
	bool next(KeyValue &row);
	bool prev(KeyValue &row);
	bool last(KeyValue &row);
	bool end(KeyValue &row);
	bool fill(KeyValue &row);
	void init(const std::string_view &start, bool rev);
	virtual bool testKey(std::string_view &key) const;
};

class LevelDBIteratorPrefix: public LevelDBIteratorBase {
public:
	LevelDBIteratorPrefix(std::string &&prefix, leveldb::Iterator *iter, bool rev);
protected:
	virtual bool testKey(std::string_view &key) const;
	std::string prefix;

};

class LevelDBIteratorRange: public LevelDBIteratorBase {
public:
	LevelDBIteratorRange(const std::string_view &start, const std::string_view &end, leveldb::Iterator *iter);
protected:
	virtual bool testKey(std::string_view &key) const;
	std::string end;
	bool rev;

};


class LevelDBDatabase: public AbstractKeyValueDatabase {
public:

	LevelDBDatabase(leveldb::DB *db);
	virtual PChangeset createChangeset();
	virtual PIterator findRange(const std::string_view &prefix, bool reverse = false) ;
	virtual PIterator findRange(const std::string_view &start, const std::string_view &end) ;
	virtual bool lookup(const std::string_view &key, std::string &value) ;
	virtual bool exists(const std::string_view &key) ;
	virtual bool existsPrefix(const std::string_view &key) ;
	virtual ~LevelDBDatabase();
	leveldb::DB *getDBObject() {return db;}

protected:
	leveldb::DB *db;



};


class LevelDBChangeset: public AbstractChangeset {
public:
	LevelDBChangeset(RefCntPtr<LevelDBDatabase> db);

	virtual void put(const std::string_view &key, const std::string_view &value);
	virtual void erase(const std::string_view &key);
	virtual void commit() ;
	virtual void rollback();
	virtual void erasePrefix(const std::string_view &prefix);

protected:
	RefCntPtr<LevelDBDatabase> db;
	leveldb::WriteBatch batch;

};


}



#endif /* SRC_LIBSOFA_KVAPI_LEVELDB_IMPL_H_ */

