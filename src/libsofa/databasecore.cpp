/*
 * database.cpp
 *
 *  Created on: 5. 2. 2019
 *      Author: ondra
 */

#include <imtjson/array.h>
#include <imtjson/object.h>
#include <libsofa/databasecore.h>
#include "keyformat.h"
#include "kvapi_leveldb.h"

namespace sofadb {

DatabaseCore::DatabaseCore() {
	// TODO Auto-generated constructor stub

}

DatabaseCore::~DatabaseCore() {
	// TODO Auto-generated destructor stub
}

void DatabaseCore::open(std::string_view name) {

	maindb = open_leveldb_database(name);
	std::string key;

	idmap.clear();
	dblist.clear();

	key_db_map(key);
	Iterator iter (maindb->find_range(key));
	while (iter.getNext()) {
		Handle h;
		extract_value(iter->second,h);
		if (h > dblist.size()) dblist.resize(h+1);
		Info &nfo = *dblist[h];
		idmap[nfo.name] = h;

		key_seq(key, h);
		Iterator maxseq (maindb->find_range(key, true));
		if (maxseq.getNext()) {
			SeqNum sq;
			extract_from_key(maxseq->second,0,sq);
			nfo.nextSeqNum = sq+1;
		} else {
			nfo.nextSeqNum = 1;
		}
	}
}


DatabaseCore::Handle DatabaseCore::allocSlot() {
	std::size_t cnt = dblist.size();
	for (std::size_t i = 0; i < cnt; i++) {
		if (dblist[i]->name.empty()) return i;
	}

	dblist.resize(cnt+1);
	return cnt;
}

DatabaseCore::Handle DatabaseCore::create(const std::string_view& name) {
	auto xf = idmap.find(name);
	if (xf != idmap.end()) return xf->second;
	if (name.empty()) return invalid_handle;

	Handle h = allocSlot();
	Info &nfo = *dblist[h];
	nfo.name = name;
	nfo.nextSeqNum = 1;
	idmap.insert(std::pair(nfo.name,h));

	std::string key,value;
	key_db_map(key,name);
	serialize_value(value,h);

	PChangeset chst = maindb->createChangeset();
	chst->put(key,value);
	chst->commit();

	return h;
}

DatabaseCore::Handle DatabaseCore::getHandle(const std::string_view& name) const {
	auto f = idmap.find(name);
	if (f == idmap.end()) return invalid_handle;
	else return f->second;
}

void DatabaseCore::erase(Handle h) {

	if (h >= dblist.size() || dblist[h] == nullptr) return;

	Info &nfo = dblist[h];
	idmap.erase(nfo.name);


	onBatchClose(h,[h,n = nfo.name,this]() {
		Info &nfo = dblist[h];
		nfo.writeState.lockCount++;
		PChangeset chst = maindb->createChangeset();

		enum_database_prefixes(nfo.key, h, [&](const std::string &k) {
			Iterator iter(maindb->find_range(k));
			while (iter.getNext()) {
				chst->erase(iter->first);
			}
		});
		key_db_map(nfo.key, n);
		chst->erase(nfo.key);
		chst->commit();
		nfo.writeState.lockCount--;
	});

	nfo.name.clear();


}

bool DatabaseCore::beginBatch(Handle h, bool exclusive ) {
	Info &nfo = dblist[h];
	if (h>=dblist.size() || nfo.name.empty()) return false;
	if (exclusive && nfo.writeState.lockCount) return false;
	++nfo.writeState.lockCount;
	if (nfo.writeState.curBatch == nullptr)
		nfo.writeState.curBatch = maindb->createChangeset();
	return true;
}

bool DatabaseCore::storeUpdate(Handle h, const RawDocument& doc) {

	//if we cannot open batch, exit with error
	if (!beginBatch(h)) return false;

	Info &nfo = dblist[h];
	//get current batch
	PChangeset chng = nfo.writeState.curBatch;
	//generate new sequence id
	auto seqid = nfo.nextSeqNum++;
	//prepare value header (contains revision and seqid)
	serialize_value(nfo.value, doc.revision, seqid);
	//append serialized json
	serializeJSON(doc.content, nfo.value);
	//prepare key (contains docid)
	key_docs(nfo.key,h,doc.docId);

	//if there is already previous revision
	//we need to put it to the historical revision index
	//this reduces performance by need to load and then store the whole document
	//but the previous revision should be already in cache, because
	//we needed it to create RawDocument
	//and having current revision indexed by docid only increases
	//lookup performance

	//so try to get current revision
	if (maindb->lookup(nfo.key, nfo.value2)) {
		RevID currev;
		SeqNum curseq;
		//extract just header - we don't need to parse whole JSON
		extract_value(nfo.value2,currev, curseq);
		//check: if revisions are same, this is error, stop here
		if (currev == doc.revision) {
			endBatch(h);
			return false;
		}
		//prepare key for historical index
		key_doc_revs(nfo.key2, h, doc.docId, currev);
		//put document to the historical table
		chng->put(nfo.key2,nfo.value2);
		//generate its seq number
		key_seq(nfo.key2,h,curseq);
		//erase seq_number, because only current revisions are streamed
		chng->erase(nfo.key2);
	}
	//now put the document to the storage
	chng->put(nfo.key,nfo.value);
	//generate key for sequence
	key_seq(nfo.key, h, seqid);
	//serialize document ID
	serialize_value(nfo.value,doc.docId);
	//put it to database
	chng->put(nfo.key,nfo.value);
	//all done
	endBatch(h);
	return true;
}

void DatabaseCore::endBatch(Handle h) {
	Info &nfo = dblist[h];
	if (h>=dblist.size()) return;

	if (nfo.writeState.lockCount > 0)
		--nfo.writeState.lockCount;
	while (nfo.writeState.lockCount==0 && !nfo.writeState.waiting.empty()) {
		auto t = nfo.writeState.waiting.front();
		nfo.writeState.waiting.pop();
		t();
	}
}

bool DatabaseCore::onBatchClose(Handle h, CloseBatchCallback cb) {
	Info &nfo = dblist[h];
	if (h>=dblist.size() || nfo.name.empty()) return false;
	if (nfo.writeState.lockCount == 0) cb();
	else nfo.writeState.waiting.push(cb);
	return true;
}

} /* namespace sofadb */
