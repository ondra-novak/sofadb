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
	std::lock_guard<std::recursive_mutex> _(lock);

	maindb = open_leveldb_database(name);
	std::string key;

	idmap.clear();
	dblist.clear();

	key_db_map(key);
	Handle maxHandle = 31;
	Iterator iter (maindb->find_range(key));
	std::vector<std::pair<std::string, Handle> > tmpstorage;

	while (iter.getNext()) {
		Handle h;
		std::string name;
		extract_from_key(iter->first, key.length(), name);
		extract_value(iter->second,h);
		tmpstorage.push_back(std::pair(std::move(name), h));
		if (h > maxHandle) maxHandle = h;
	}
	dblist.resize(maxHandle+1);
	for (auto &&c:tmpstorage) {
		auto nfo = std::make_unique<Info>();
		nfo->name = c.first;
		idmap[nfo->name] = c.second;

		key_seq(key, c.second);
		Iterator maxseq (maindb->find_range(key, true));

		if (maxseq.getNext()) {
			SeqNum sq;
			extract_value(maxseq->second,sq);
			nfo->nextSeqNum = sq+1;
		} else {
			nfo->nextSeqNum = 1;
		}

		dblist[c.second] = std::move(nfo);
	}

}


DatabaseCore::Handle DatabaseCore::allocSlot() {
	std::size_t cnt = dblist.size();
	for (std::size_t i = 0; i < cnt; i++) {
		if (dblist[i] == nullptr) {
			dblist[i] = std::make_unique<Info>();
			return i;
		}
	}
	dblist.resize(cnt+1);
	dblist[cnt] = std::make_unique<Info>();
	return cnt;
}

DatabaseCore::Handle DatabaseCore::create(const std::string_view& name) {

	std::lock_guard<std::recursive_mutex> _(lock);

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

	std::lock_guard<std::recursive_mutex> _(lock);

	auto f = idmap.find(name);
	if (f == idmap.end()) return invalid_handle;
	else return f->second;
}

void DatabaseCore::erase(Handle h) {

	PInfo nfo = getDatabaseState(h);
	if (nfo == nullptr) return ;

	idmap.erase(nfo->name);

	onBatchClose(h,[h,this]() {
		std::unique_ptr<Info> nfo (std::move(dblist[h]));
		if (nfo == nullptr) return;

		PChangeset chst = maindb->createChangeset();
		enum_database_prefixes(nfo->key, h, [&](const std::string &k) {
			Iterator iter(maindb->find_range(k));
			while (iter.getNext()) {
				chst->erase(iter->first);
			}
		});
		key_db_map(nfo->key, nfo->name);
		chst->erase(nfo->key);
		chst->commit();
		nfo->writeState.lockCount = 0;
		flushWriteState(nfo->writeState);
	});



}

DatabaseCore::PInfo DatabaseCore::getDatabaseState(Handle h) {
	std::lock_guard<std::recursive_mutex> _(lock);
	if (h >= dblist.size()) return nullptr;
	else return PInfo(dblist[h].get());
}

void DatabaseCore::flushWriteState(WriteState &st) {
	while (!st.waiting.empty() && st.lockCount == 0) {
		auto &&fn = std::move(st.waiting.front());
		st.waiting.pop();
		fn();
	}
}

bool DatabaseCore::beginBatch(Handle h, bool exclusive ) {
	PInfo nfo = getDatabaseState(h);
	if (nfo == nullptr) return false;
	if (exclusive && nfo->writeState.lockCount) return false;
	beginBatch(nfo);
	return true;
}

PChangeset DatabaseCore::beginBatch(PInfo &nfo) {
	++nfo->writeState.lockCount;
	if (nfo->writeState.curBatch == nullptr)
		nfo->writeState.curBatch = maindb->createChangeset();
	return nfo->writeState.curBatch;
}

bool DatabaseCore::storeUpdate(Handle h, const RawDocument& doc) {

	PInfo nfo = getDatabaseState(h);
	if (nfo == nullptr) return false;

	PChangeset chng = beginBatch(nfo);

	//generate new sequence id
	auto seqid = nfo->nextSeqNum++;
	//prepare value header (contains revision and seqid)
	serialize_value(nfo->value, doc.revision, seqid);
	//append serialized json
	serializeJSON(doc.content, nfo->value);
	//prepare key (contains docid)
	key_docs(nfo->key,h,doc.docId);

	//if there is already previous revision
	//we need to put it to the historical revision index
	//this reduces performance by need to load and then store the whole document
	//but the previous revision should be already in cache, because
	//we needed it to create RawDocument
	//and having current revision indexed by docid only increases
	//lookup performance

	//so try to get current revision
	if (maindb->lookup(nfo->key, nfo->value2)) {
		RevID currev;
		SeqNum curseq;
		//extract just header - we don't need to parse whole JSON
		extract_value(nfo->value2,currev, curseq);
		//check: if revisions are same, this is error, stop here
		if (currev == doc.revision) {
			endBatch(h);
			return false;
		}
		//prepare key for historical index
		key_doc_revs(nfo->key2, h, doc.docId, currev);
		//put document to the historical table
		chng->put(nfo->key2,nfo->value2);
		//generate its seq number
		key_seq(nfo->key2,h,curseq);
		//erase seq_number, because only current revisions are streamed
		chng->erase(nfo->key2);
	}
	//now put the document to the storage
	chng->put(nfo->key,nfo->value);
	//generate key for sequence
	key_seq(nfo->key, h, seqid);
	//serialize document ID
	serialize_value(nfo->value,doc.docId);
	//put it to database
	chng->put(nfo->key,nfo->value);
	//all done
	endBatch(nfo);
	return true;
}

void DatabaseCore::endBatch(Handle h) {
	PInfo nfo = getDatabaseState(h);
	if (nfo == nullptr) return ;
	endBatch(nfo);
}

bool DatabaseCore::findDoc(Handle h, const std::string_view& docid, DocumentInfo& content, bool only_header) {
	std::string key, value;
	key_docs(key, h, docid);
	if (!maindb->lookup(key,value)) return false;
	value2document(value, content, only_header);
	return true;
}

bool DatabaseCore::findDoc(Handle h, const std::string_view& docid, RevID revid,
		DocumentInfo& content, bool only_header) {
	std::string key, value;
	key_docs(key, h, docid);
	if (!maindb->lookup(key,value)) return false;
	value2document(value, content, true);
	if (content.revision != revid) {
		key_doc_revs(key,h,docid,revid);
		if (!maindb->lookup(key,value)) return false;
	}
	value2document(value, content, only_header);
	return true;
}

bool DatabaseCore::enumAllRevisions(Handle h, const std::string_view& docid,
		std::function<void(const DocumentInfo&)> callback, bool only_header) {

	std::string key;
	DocumentInfo docinfo;
	if (!findDoc(h, docid, docinfo, only_header)) return false;
	callback(docinfo);
	key_doc_revs(key, h, docid);
	Iterator iter(maindb->find_range(key));
	while (iter.getNext()) {
		value2document(iter->second, docinfo, only_header);
		callback(docinfo);
	}
	return true;
}

void DatabaseCore::eraseDoc(Handle h, const std::string_view& docid) {
	PInfo nfo = getDatabaseState(h);
	if (nfo == nullptr) return ;

	PChangeset chng = beginBatch(nfo);
	enumAllRevisions(h, docid,[&](const DocumentInfo &docinf) {
		key_doc_revs(nfo->key, h, docid, docinf.revision);
		chng->erase(nfo->key);
		key_seq(nfo->key, h, docinf.seq_number);
		chng->erase(nfo->key);
	});
	key_docs(nfo->key,h, docid);
	chng->erase(nfo->key);
	endBatch(nfo);

	//TODO remove from view

}

void DatabaseCore::eraseHistoricalDoc(Handle h, const std::string_view& docid,
		RevID revision) {
	PInfo nfo = getDatabaseState(h);
	if (nfo == nullptr) return ;
	PChangeset chng = beginBatch(nfo);
	key_doc_revs(nfo->key, h, docid, revision);
	chng->erase(nfo->key);
	endBatch(nfo);

}

void DatabaseCore::enumDocs(Handle h, const std::string_view& prefix,
		 bool reversed, std::function<void(const DocumentInfo&)> callback, bool header_only) {

	DocumentInfo dinfo;
	std::string key;
	key_docs(key,h,prefix);
	Iterator iter(maindb->find_range(key, reversed));
	auto skip = key.length() - prefix.length();
	while (iter.getNext()) {
		value2document(iter->second, dinfo,header_only);
		extract_from_key(iter->first, skip, dinfo.docid);
		callback(dinfo);
	}

}

void DatabaseCore::enumDocs(Handle h, const std::string_view& start_include,
		const std::string_view& end_exclude,
		std::function<void(const DocumentInfo&)> callback, bool header_only) {

	DocumentInfo dinfo;
	std::string key1, key2;
	key_docs(key1,h,start_include);
	key_docs(key2,h,end_exclude);
	Iterator iter(maindb->find_range(key1, key2));
	auto skip = key1.length() - start_include.length();
	while (iter.getNext()) {
		value2document(iter->second, dinfo,header_only);
		extract_from_key(iter->first, skip, dinfo.docid);
		callback(dinfo);
	}

}

bool DatabaseCore::findDocBySeqNum(Handle h, SeqNum seqNum, DocID &docid) {
	std::string key, value;
	key_seq(key, h,seqNum);
	if (!maindb->lookup(key, value)) return false;
	extract_value(docid);
	return true;

}

SeqNum DatabaseCore::readChanges(Handle h, SeqNum from,
		std::function<bool(const DocID&, const SeqNum&)>&& fn)  {
	std::string key1, key2;
	key_seq(key1, h, from+1);
	key_seq(key2, h);
	std::size_t skip = key2.length();
	key_seq(key2, h+1, 0);
	Iterator iter(maindb->find_range(key1, key2));
	DocID docId;
	while (iter.getNext()) {
		DocID docId;
		SeqNum seq;
		extract_value(iter->second,docId);
		extract_from_key(iter->first, skip, seq);
		if (!fn(docId, from)) return from;
	}
	return from;
}


void DatabaseCore::endBatch(PInfo &nfo) {
	if (nfo->writeState.lockCount > 0) --nfo->writeState.lockCount;
	flushWriteState(nfo->writeState);
}


bool DatabaseCore::onBatchClose(Handle h, CloseBatchCallback cb) {
	PInfo nfo = getDatabaseState(h);
	if (nfo == nullptr) return false;
	if (nfo->writeState.lockCount == 0) cb();
	else nfo->writeState.waiting.push(cb);
	return true;
}
void DatabaseCore::value2document(const std::string_view &value, DocumentInfo &doc, bool only_header) {
	const char *data;
	extract_value(value,doc.revision, doc.seq_number, data);
	if (!only_header) {
		doc.content = parseJSON(value.substr(data - value.data()));
	}
}


} /* namespace sofadb */
