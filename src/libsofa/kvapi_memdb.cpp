/*
 * kvapi_memdb.cpp
 *
 *  Created on: 25. 2. 2019
 *      Author: ondra
 */

#include "kvapi_memdb.h"

namespace sofadb {

MemDB::MemDB() {
}

std::string prefixLastKey(const std::string_view &prefix);

PChangeset MemDB::createChangeset() {
	return new MemDBChangeset(this);
}

PIterator MemDB::findRange(const std::string_view& prefix,	bool reverse) {
	Sync _(lock);
	auto begin = prefix.empty()?data.begin():data.lower_bound(json::StrViewA(prefix));
	auto end = prefix.empty()?data.end():data.lower_bound(prefixLastKey(prefix));
	iterCount++;
	if (reverse) {
		return new MemDBIterator<DataMap::reverse_iterator>(
				std::make_reverse_iterator(begin),
				std::make_reverse_iterator(end),
				this);

	} else {
		return new MemDBIterator<DataMap::iterator>(begin,end,this);
	}
}

PIterator MemDB::findRange(const std::string_view& start, const std::string_view& stop) {
	Sync _(lock);
	auto begin = data.lower_bound(json::StrViewA(start));
	auto end = data.lower_bound(json::StrViewA(stop));
	if (start > stop) {
		return new MemDBIterator<DataMap::reverse_iterator>(
				std::make_reverse_iterator(begin),
				std::make_reverse_iterator(end),
				this);

	} else {
		return new MemDBIterator<DataMap::iterator>(begin,end,this);
	}
}

bool MemDB::lookup(const std::string_view& key, std::string& value) {
	Sync _(lock);
	auto iter = data.find(json::StrViewA(key));
	if (iter == data.end()) return false;
	if (iter->second.valid) {
		value = iter->second.data.str();
		return true;
	} else {
		return false;
	}
}

bool MemDB::exists(const std::string_view& key) {
	Sync _(lock);
	auto iter = data.find(json::StrViewA(key));
	return iter != data.end();
}

bool MemDB::existsPrefix(const std::string_view& key) {
	Sync _(lock);
	auto iter = data.lower_bound(json::StrViewA(key));
	if (iter == data.end()) return false;
	return iter->first.substr(0,key.length()) == json::StrViewA(key);
}

void MemDB::destroy() {
	//Nothing here, database has no files
}


void MemDBChangeset::put(const std::string_view& key, const std::string_view& value) {
	batchWrite.push_back(Command(0,key));
	batchWrite.push_back(Command(1,value));
}

void MemDBChangeset::erase(const std::string_view& key) {
	batchWrite.push_back(Command(2,key));
}
void MemDBChangeset::erase(const json::String& key) {
	batchWrite.push_back(Command(2,key));
}

void MemDBChangeset::commit() {
	memdb->commitBatch(batchWrite);
}

void MemDBChangeset::rollback() {
	batchWrite.clear();
}

void MemDBChangeset::erasePrefix(const std::string_view& prefix) {
	MemDB::Sync _(memdb->lock);
	json::StrViewA p(prefix);
	auto iter = memdb->data.lower_bound(json::StrViewA(prefix));
	while (iter != memdb->data.end() && iter->first.substr(0,prefix.length()) == p) {
		erase(iter->first);
		++iter;
	}
}

template<typename iterator, typename Owner>
MemDBIteratorBase<iterator,Owner>::MemDBIteratorBase(iterator begin, iterator end, RefCntPtr<Owner> owner)
	:begin(begin)
	,end(end)
	,owner(owner)

{
}

template<typename iterator>
MemDBIterator<iterator>::~MemDBIterator() {
	MemDB::Sync _(this->owner->lock);
	if (--this->owner->iterCount == 0) {
		for (auto &&c: this->owner->eraseBatch) {
			this->owner->data.erase(c);
		}
		this->owner->eraseBatch.clear();
	}
}

template<typename iterator, typename Owner>
bool MemDBIteratorBase<iterator,Owner>::next() {
	MemDB::Sync _(owner->lock);
	if (begin == end) return false;
	++begin;
	return true;
}
template<typename iterator, typename Owner>
const MemDBCommon::Key &MemDBIteratorBase<iterator,Owner>::getKey() const {
	return begin->first;
}
template<typename iterator, typename Owner>
const MemDBCommon::Value &MemDBIteratorBase<iterator,Owner>::getValue() const {
	return begin->second;
}
template<typename iterator, typename Owner>
bool MemDBIteratorBase<iterator,Owner>::hasItems() const {
	return begin != end;
}


template<typename iterator, typename Owner>
bool MemDBIteratorBase<iterator,Owner>::getNext(KeyValue& row) {
	MemDB::Sync _(owner->lock);
	bool valid;
	do {
		if (begin == end) return false;
		valid = begin->second.valid;
		last_value = begin->second.data.str();
		row.first = json::StrViewA(begin->first);
		row.second = last_value;
		++begin;
	}
	while (!valid);
	return true;
}

void MemDB::commitBatch(std::vector<MemDBChangeset::Command> &batch) {
	MemDB::Sync _(lock);
	json::String key;
	for (auto &&c : batch) {
		switch (c.first) {
		case 0: std::swap(key, c.second);break;
		case 1: {
			MemDB::Value &val = data[key];
			for (auto &c: snapshots) c->copyOnWrite(key,val);
			val.data =  c.second;
			val.valid = true;
			break;
			}
		case 2: {
			auto iter = data.find(c.second);
			if (iter != data.end()) {
				for (auto &c: snapshots) c->copyOnWrite(iter->first,iter->second);
				if (iterCount) {
					iter->second.valid = false;
					iter->second.data = json::String();
					eraseBatch.push_back(c.second);
				} else {
					data.erase(iter);
				}
			}

		} break;
		}
	}
	batch.clear();

}

PKeyValueDatabaseSnapshot MemDB::createSnapshot() {
	MemDBSnapshot *msnp = new MemDBSnapshot(this);
	PKeyValueDatabaseSnapshot snp = msnp;
	addSnapshot(msnp);
	return snp;
}

MemDBSnapshot::MemDBSnapshot(RefCntPtr<MemDB> owner)
	:owner(owner){

}

PIterator MemDBSnapshot::findRange(const std::string_view& prefix, bool reverse) {
	Sync _(lock);
	PMemDBIterBase iter1 = PMemDBIterBase::staticCast(owner->findRange(prefix,reverse));

	auto begin = prefix.empty()?data.begin():data.lower_bound(json::StrViewA(prefix));
	auto end = prefix.empty()?data.end():data.lower_bound(prefixLastKey(prefix));

	if (reverse) {
		PMemDBIterBase iter2 =  new MemDBIteratorBase<DataMap::reverse_iterator, MemDBSnapshot>(
				std::make_reverse_iterator(begin),
				std::make_reverse_iterator(end),
				this);
		return new MemDBSnapshotIterator<std::greater<json::String> >(iter1,iter2,std::greater<json::String>());
	} else {
		PMemDBIterBase iter2 =  new MemDBIteratorBase<DataMap::iterator, MemDBSnapshot>(begin,end,this);
		return new MemDBSnapshotIterator<std::less<json::String> >(iter1,iter2,std::less<json::String>());
	}

}

PIterator MemDBSnapshot::findRange(const std::string_view& start, const std::string_view& stop) {
	Sync _(lock);
	PMemDBIterBase iter1 = PMemDBIterBase::staticCast(owner->findRange(start,stop));
	auto begin = data.lower_bound(json::StrViewA(start));
	auto end = data.lower_bound(json::StrViewA(stop));
	if (start > stop) {
		PMemDBIterBase iter2 = new MemDBIteratorBase<DataMap::reverse_iterator,MemDBSnapshot>(
				std::make_reverse_iterator(begin),
				std::make_reverse_iterator(end),
				this);
		return new MemDBSnapshotIterator<std::greater<json::String> >(iter1,iter2,std::greater<json::String>());
	} else {
		PMemDBIterBase iter2 = new MemDBIteratorBase<DataMap::iterator,MemDBSnapshot>(begin,end,this);
		return new MemDBSnapshotIterator<std::less<json::String> >(iter1,iter2,std::less<json::String>());
	}
}

bool MemDBSnapshot::lookup(const std::string_view& key, std::string& value) {
	Sync _(lock);
	auto iter = data.find(json::StrViewA(key));
	if (iter == data.end()) return owner->lookup(key,value);
	if (iter->second.valid) {
		value = iter->second.data.str();
		return true;
	} else {
		return false;
	}

}

bool MemDBSnapshot::exists(const std::string_view& key) {
	Sync _(lock);
	auto iter = data.find(json::StrViewA(key));
	if (iter != data.end()) {
		return iter->second.valid;
	} else {
		return owner->exists(key);
	}
}

MemDBSnapshot::~MemDBSnapshot() {
	owner->removeSnapshot(this);
}

bool MemDBSnapshot::existsPrefix(const std::string_view& key) {
	Iterator iter ( findRange(key,false));
	return iter.getNext();
}

void MemDBSnapshot::copyOnWrite(const Key& key, const Value& value) {
	Sync _(lock);
	auto iter = data.find(key);
	if (iter == data.end()) {
		data.insert(std::pair(key,value));
	}
}

void MemDB::addSnapshot(MemDBSnapshot* snapshot) {
	Sync _(lock);

	snapshots.push_back(snapshot);

}

void MemDB::removeSnapshot(MemDBSnapshot* snapshot) {
	Sync _(lock);
	auto iter = std::find(snapshots.begin(),snapshots.end(),snapshot);
	if (iter != snapshots.end()) {
		snapshots.erase(iter);
	}

}

template< typename Cmp>
inline MemDBSnapshotIterator<Cmp>::MemDBSnapshotIterator(
		PIter iter1, PIter iter2, Cmp cmp):iter1(iter1),iter2(iter2),cmp(cmp) {
}

template<typename Cmp>
inline bool sofadb::MemDBSnapshotIterator<Cmp>::getNext(KeyValue& row) {

	bool hv1 = iter1->hasItems();
	bool hv2 = iter2->hasItems();
	while (hv1 && hv2) {
		auto k1 = iter1->getKey();
		auto k2 = iter2->getKey();
		if (cmp(k1, k2)) {
			if (iter1->getValue().valid) {
				iter1->getNext(row);
				return true;
			} else {
				iter1->next();
			}
		} else if (cmp(k2 , k1)) {
			if (iter1->getValue().valid) {
				iter2->getNext(row);
				return true;
			} else {
				iter2->next();
			}
		} else {
			if (iter2->getValue().valid) {
				iter2->getNext(row);
				iter1->next();
				return true;
			} else {
				iter1->next();
				iter2->next();
			}
		}
	}
	if (hv1) {
		return iter1->getNext(row);
	} else if (hv2) {
		return iter2->getNext(row);
	} else {
		return false;
	}

}

}
