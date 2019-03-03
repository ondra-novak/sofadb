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
	auto begin = prefix.empty()?data.begin():data.lower_bound(prefix);
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
	auto begin = data.lower_bound(start);
	auto end = data.lower_bound(stop);
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
	auto iter = data.find(key);
	if (iter == data.end()) return false;
	value = iter->second;
	return true;
}

bool MemDB::exists(const std::string_view& key) {
	Sync _(lock);
	auto iter = data.find(key);
	return iter != data.end();
}

bool MemDB::existsPrefix(const std::string_view& key) {
	Sync _(lock);
	auto iter = data.lower_bound(key);
	if (iter == data.end()) return false;
	return std::string_view(iter->second).substr(0,key.length()) == key;
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

void MemDBChangeset::commit() {
	MemDB::Sync _(memdb->lock);
	std::string key;
	for (auto &&c : batchWrite) {
		switch (c.first) {
		case 0: std::swap(key, c.second);break;
		case 1: {
			std::string &val = memdb->data[std::move(key)];
			std::swap(val, c.second);
			break;
			}
		case 2: if (memdb->iterCount) memdb->eraseBatch.push_back(std::move(c.second));
				else memdb->data.erase(c.second);
				break;
		}
	}
	batchWrite.clear();
}

void MemDBChangeset::rollback() {
	batchWrite.clear();
}

void MemDBChangeset::erasePrefix(const std::string_view& prefix) {
	MemDB::Sync _(memdb->lock);
	auto iter = memdb->data.lower_bound(prefix);
	while (iter != memdb->data.end() && std::string_view(iter->first).substr(0,prefix.length()) == prefix) {
		erase(iter->first);
		++iter;
	}
}

template<typename iterator>
MemDBIterator<iterator>::MemDBIterator(iterator begin, iterator end, RefCntPtr<MemDB> owner)
	:begin(begin)
	,end(end)
	,owner(owner)

{
}

template<typename iterator>
MemDBIterator<iterator>::~MemDBIterator() {
	MemDB::Sync _(owner->lock);
	if (--owner->iterCount == 0) {
		for (auto &&c: owner->eraseBatch) {
			owner->data.erase(c);
		}
		owner->eraseBatch.clear();
	}
}

template<typename iterator>
bool MemDBIterator<iterator>::getNext(KeyValue& row) {
	MemDB::Sync _(owner->lock);
	if (begin == end) return false;
	last_value = begin->second;
	row.first = begin->first;
	row.second = last_value;
	++begin;
	return true;
}


}
