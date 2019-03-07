/*
 * kvapi_memdb.h
 *
 *  Created on: 25. 2. 2019
 *      Author: ondra
 */

#ifndef SRC_LIBSOFA_KVAPI_MEMDB_H_
#define SRC_LIBSOFA_KVAPI_MEMDB_H_

#include <imtjson/string.h>
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
	void erase(const json::String &key);

	using Command = std::pair<unsigned char,json::String>;

protected:
	RefCntPtr<MemDB> memdb;
	std::vector<Command> batchWrite;
};

class MemDBCommon {
public:
	using Key = json::String;
	struct Value {
		bool valid = false;
		json::String data;
	};
	using Sync = std::unique_lock<std::recursive_mutex>;
	using DataMap = std::map<Key, Value, std::less<json::StrViewA> >;

};


///Memory snapshot
/**
 * @note we will only emulate snapshot by keeping lock to avoid changes from other threads
 * It expects that snapshot is not kept for long time
 */
class MemDBSnapshot: public AbstractKeyValueDatabaseSnapshot, public MemDBCommon {
public:
	MemDBSnapshot(RefCntPtr<MemDB> owner);
	~MemDBSnapshot();

	virtual PIterator findRange(const std::string_view &prefix, bool reverse = false);
	virtual PIterator findRange(const std::string_view &start, const std::string_view &end);
	virtual bool lookup(const std::string_view &key, std::string &value);
	virtual bool exists(const std::string_view &key);
	virtual bool existsPrefix(const std::string_view &key);
	void copyOnWrite(const Key &key, const Value &value);
protected:
	RefCntPtr<MemDB> owner;
	std::recursive_mutex lock;
	DataMap data;

	template<typename,typename> friend class MemDBIteratorBase;

};



class MemDB: public AbstractKeyValueDatabase, public MemDBCommon {
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

	void commitBatch(std::vector<MemDBChangeset::Command> &batch);

	void addSnapshot(MemDBSnapshot *snapshot);
	void removeSnapshot(MemDBSnapshot *snapshot);
protected:




	std::recursive_mutex lock;
	DataMap data;
	unsigned int iterCount = 0;
	std::vector<json::String> eraseBatch;
	std::vector<MemDBSnapshot *> snapshots;


	friend class MemDBChangeset;
	template<typename> friend class MemDBIterator;
	template<typename,typename> friend class MemDBIteratorBase;
	friend class MemDBSnapshot;

};

class MemDBIteratorGen: public  AbstractIterator {
public:
	virtual bool hasItems() const = 0;
	virtual const MemDBCommon::Key &getKey() const = 0;
	virtual const MemDBCommon::Value &getValue() const = 0;
	virtual bool next() = 0;

};

template<typename iterator, typename Owner>
class MemDBIteratorBase: public MemDBIteratorGen {
public:
	MemDBIteratorBase(iterator begin,
			iterator end,
			RefCntPtr<Owner> owner);


	virtual bool getNext(KeyValue &row);
	virtual bool hasItems() const ;
	virtual const MemDBCommon::Key &getKey() const;
	virtual const MemDBCommon::Value &getValue() const;
	virtual bool next() ;

protected:
	iterator begin;
	iterator end;
	RefCntPtr<Owner> owner;

	std::string last_value;
};



template<typename iterator>
class MemDBIterator: public MemDBIteratorBase<iterator,MemDB> {
public:
	using MemDBIteratorBase<iterator,MemDB>::MemDBIteratorBase;

	~MemDBIterator();
};

using PMemDBIterBase =  RefCntPtr<MemDBIteratorGen>;


template<typename Cmp>
class MemDBSnapshotIterator: public AbstractIterator {
public:
	using PIter = RefCntPtr<MemDBIteratorGen>;
	MemDBSnapshotIterator(PIter iter1, PIter iter2, Cmp cmp);


	virtual bool getNext(KeyValue &row);

protected:
	PIter iter1;
	PIter iter2;
	Cmp cmp;
};

}


#endif /* SRC_LIBSOFA_KVAPI_MEMDB_H_ */
