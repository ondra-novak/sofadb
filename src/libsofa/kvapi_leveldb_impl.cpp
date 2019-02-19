#include <memory>
#include "kvapi_leveldb_impl.h"
#include "kvapi_leveldb.h"

namespace sofadb {

static const std::string destroy_key="~destroy";

LevelDBDatabase::LevelDBDatabase(leveldb::DB *db, const std::string_view &name):db(db),name(name) {

}


inline leveldb::Slice str2slice(const std::string_view &w) {
	return leveldb::Slice(w.data(),w.length());
}
inline std::string_view slice2str(const leveldb::Slice &w) {
	return std::string_view(w.data(),w.size());
}


static std::string prefixLastKey(const std::string_view &prefix) {
	std::string lastKey (prefix);
	while (!lastKey.empty()) {
		unsigned char c = static_cast<unsigned char>(lastKey.back());
		lastKey.pop_back();
		if (++c) {
			lastKey.push_back(c);
			break;
		}
	}
	return lastKey;
}

PChangeset LevelDBDatabase::createChangeset() {
	return new LevelDBChangeset(this);
}

PIterator LevelDBDatabase::findRange(const std::string_view& prefix, bool reverse) {
	leveldb::ReadOptions opt;
	opt.fill_cache = false;
	return new LevelDBIteratorPrefix(std::string(prefix), db->NewIterator(opt), reverse);
}

PIterator LevelDBDatabase::findRange(const std::string_view& start,
		const std::string_view& end) {
	leveldb::ReadOptions opt;
	opt.fill_cache = false;
	return new LevelDBIteratorRange(start, end, db->NewIterator(opt));
}

/*PIterator LevelDBDatabase::find(const std::string_view& prefix) {
	db->
}*/

bool LevelDBDatabase::lookup(const std::string_view& key, std::string& value) {
	leveldb::ReadOptions opt;
	opt.fill_cache = false;
	leveldb::Status s = db->Get(opt, str2slice(key),&value);
	if (s.ok()) return true;
	if (s.IsNotFound()) return false;
	throw LevelDBException(s);
}

bool LevelDBDatabase::exists(const std::string_view& key) {
	std::string tmp;
	return lookup(key,tmp);
}

bool LevelDBDatabase::existsPrefix(const std::string_view& key) {
	leveldb::ReadOptions opt;
	opt.fill_cache = false;
	std::unique_ptr<leveldb::Iterator> iter(db->NewIterator(opt));
	iter->Seek(str2slice(key));
	if (iter->Valid()) return slice2str(iter->key()).substr(0,key.length()) == key;
	else return false;

}


LevelDBDatabase::~LevelDBDatabase() {
	bool d = isDestroyed();
	delete db;
	if (d) leveldb::DestroyDB(name, leveldb::Options());
}


LevelDBChangeset::LevelDBChangeset(RefCntPtr<LevelDBDatabase> db):db(db) {

}

void LevelDBChangeset::put(const std::string_view& key, const std::string_view& value) {
	batch.Put(str2slice(key),str2slice(value));
}

void LevelDBChangeset::erase(const std::string_view& key) {
	batch.Delete(str2slice(key));
}

void LevelDBChangeset::commit() {
	leveldb::Status st = db->getDBObject()->Write(leveldb::WriteOptions(),&batch);
	batch.Clear();
	if (!st.ok()) throw LevelDBException(st);
}

void LevelDBChangeset::rollback() {
	batch.Clear();
}

void LevelDBChangeset::erasePrefix(const std::string_view& prefix) {
	leveldb::ReadOptions opt;
	opt.fill_cache = false;
	auto len = prefix.length();
	std::unique_ptr<leveldb::Iterator> iter(db->getDBObject()->NewIterator(opt));
	iter->Seek(str2slice(prefix));
	auto st = iter->status();
	if (!st.ok()) throw LevelDBException(st);
	while(iter->Valid()) {
		auto k = iter->key();
		if (slice2str(k).substr(0,len) != prefix) break;
		batch.Delete(k);
		iter->Next();
		st = iter->status();
		if (!st.ok()) throw LevelDBException(st);
	}
}

LevelDBIteratorBase::LevelDBIteratorBase(leveldb::Iterator *iter)
	:iter(iter)
{
}

void LevelDBIteratorBase::init(const std::string_view &start, bool rev) {
	if (rev) {
		if (start.empty()) iter->SeekToLast();
		else iter->Seek(str2slice(start));
		if (iter->Valid()) iter->Prev();
		get_next=&LevelDBIteratorBase::last;
	} else {
		if (start.empty()) iter->SeekToFirst();
		else iter->Seek(str2slice(start));
		get_next=&LevelDBIteratorBase::first;
	}

}

bool LevelDBIteratorBase::fill(KeyValue &row) {
	auto st = iter->status();
	if (!st.ok()) throw LevelDBException(st);
	if (iter->Valid()) {
		row.first = slice2str(iter->key());
		row.second = slice2str(iter->value());
		if (testKey(row.first)) {
			return true;
		}
	}
	get_next = &LevelDBIteratorBase::end;
	return false;

}
bool LevelDBIteratorBase::first(KeyValue &row) {
	get_next = &LevelDBIteratorBase::next;
	return fill(row);
}
bool LevelDBIteratorBase::next(KeyValue &row) {
	iter->Next();
	return fill(row);
}
bool LevelDBIteratorBase::last(KeyValue &row){
	get_next = &LevelDBIteratorBase::prev;
	return fill(row);
/*
	iter->Seek(lastKey);
	if (!fill(row)) return false;
	iter->Prev();
*/
}

bool LevelDBIteratorBase::prev(KeyValue &row){
	iter->Prev();
	return fill(row);

}
bool LevelDBIteratorBase::getNext(KeyValue &row) {
	return (this->*get_next)(row);
}
bool LevelDBIteratorBase::end(KeyValue &) {
	return false;
}

LevelDBIteratorPrefix::LevelDBIteratorPrefix(std::string &&prefix, leveldb::Iterator *iter, bool rev)
:LevelDBIteratorBase(iter),prefix(std::move(prefix))
{

	if (rev) {
		init(prefixLastKey(this->prefix),true);
	} else {
		init(this->prefix, false);
	}
}
bool LevelDBIteratorPrefix::testKey(std::string_view &key) const {
	return prefix.empty() || key.substr(0,prefix.length()) == prefix;
}

LevelDBIteratorRange::LevelDBIteratorRange(const std::string_view &start, const std::string_view &end, leveldb::Iterator *iter)
	:LevelDBIteratorBase(iter),end(end) {

	this->rev = start > end;
	init(start, this->rev);

}
bool LevelDBIteratorRange::testKey(std::string_view &key) const {
	return rev?(key > end):(key < end);
}

void LevelDBDatabase::destroy() {
	db->Put(leveldb::WriteOptions(), destroy_key, leveldb::Slice(nullptr,0));
}

bool LevelDBDatabase::isDestroyed() const {
	std::string dummy;
	return db->Get(leveldb::ReadOptions(), destroy_key, &dummy).ok();
}

}
