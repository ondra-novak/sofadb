/*
 * kvapi_memdb.h
 *
 *  Created on: 25. 2. 2019
 *      Author: ondra
 */

#ifndef SRC_LIBSOFA_KVAPI_MEMDB_H_
#define SRC_LIBSOFA_KVAPI_MEMDB_H_

#include "kvapi.h"
#include <map>
#include <memory>
#include <mutex>
#include <vector>


namespace sofadb {

class MemDB;
template<typename> class MemDBIterator;

class MemDBChangeset: public AbstractChangeset {
public:
	MemDBChangeset(RefCntPtr<MemDB> memdb):memdb(memdb) {}
	virtual void put(const std::string_view &key, const std::string_view &value);
	virtual void erase(const std::string_view &key);
	virtual void commit();
	virtual void rollback();
	virtual void erasePrefix(const std::string_view &prefix);

protected:
	RefCntPtr<MemDB> memdb;
	using Command = std::pair<unsigned char,std::string>;
	std::vector<Command> batchWrite;
};


///Memory snapshot
/**
 * @note we will only emulate snapshot by keeping lock to avoid changes from other threads
 * It expects that snapshot is not kept for long time
 */
class MemDBSnapshot: public AbstractKeyValueDatabaseSnapshot {
public:
	MemDBSnapshot(RefCntPtr<MemDB> owner);

	virtual PIterator findRange(const std::string_view &prefix, bool reverse = false);
	virtual PIterator findRange(const std::string_view &start, const std::string_view &end);
	virtual bool lookup(const std::string_view &key, std::string &value);
	virtual bool exists(const std::string_view &key);
	virtual bool existsPrefix(const std::string_view &key);
protected:
	RefCntPtr<MemDB> owner;
	std::unique_lock<std::recursive_mutex> lock;
};



class MemDB: public AbstractKeyValueDatabase {
public:
	MemDB();

	virtual PChangeset createChangeset();
	virtual PIterator findRange(const std::string_view &prefix, bool reverse = false) ;
	virtual PIterator findRange(const std::string_view &start, const std::string_view &end) ;
	virtual bool lookup(const std::string_view &key, std::string &value) ;
	virtual bool exists(const std::string_view &key) ;
	virtual bool existsPrefix(const std::string_view &key) ;
	virtual void destroy();
	virtual PKeyValueDatabaseSnapshot createSnapshot();

protected:


	using DataMap = std::map<std::string, std::string, std::less<> >;

	std::recursive_mutex lock;
	DataMap data;
	unsigned int iterCount = 0;
	std::vector<std::string> eraseBatch;


	friend class MemDBChangeset;
	template<typename> friend class MemDBIterator;
	friend class MemDBSnapshot;

	using Sync = std::unique_lock<std::recursive_mutex>;
};


template<typename iterator>
class MemDBIterator: public AbstractIterator {
public:
	MemDBIterator(iterator begin,
			iterator end,
			RefCntPtr<MemDB> owner);

	~MemDBIterator();

	virtual bool getNext(KeyValue &row);

protected:
	iterator begin;
	iterator end;
	RefCntPtr<MemDB> owner;

	std::string last_value;
};



}


#endif /* SRC_LIBSOFA_KVAPI_MEMDB_H_ */
