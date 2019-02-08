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

DatabaseCore::DatabaseCore(PKeyValueDatabase db):maindb(db) {

	idmap.clear();
	dblist.clear();

	loadDBs();
	loadViews();
}


void DatabaseCore::loadDBs() {
	std::string key;

	key_db_map(key);
	Handle maxHandle = 31;
	Iterator iter (maindb->findRange(key));
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
		Iterator maxseq (maindb->findRange(key, true));

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

void DatabaseCore::loadViews() {

	nextViewID = 1;
	std::string key;
	key_view_state(key);
	Iterator iter(maindb->findRange(key));
	while (iter.getNext()) {
		Handle h;
		ViewID v;
		std::string_view name;
		SeqNum seqNum;

		extract_from_key(iter->first,key.length(), h, v);
		extract_value(iter->second,seqNum, name);

		auto dbst = getDatabaseState(h);
		ViewState &st = dbst->viewState[v];
		st.name = name;
		st.seqNum = seqNum;
		dbst->viewNameToID[std::string(name)] = v;
		nextViewID = std::max(nextViewID, v+1);
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

		for (auto &&w: nfo->viewState) {
			deleteViewRaw(h, w.first);
		}

		PChangeset chst = maindb->createChangeset();

		key_db_map(nfo->key, nfo->name);
		chst->erase(nfo->key);
		key_docs(nfo->key,h);
		chst->erasePrefix(nfo->key);
		key_doc_revs(nfo->key, h);
		chst->erasePrefix(nfo->key);
		key_dbconfig(nfo->key, h);
		chst->erasePrefix(nfo->key);
		key_seq(nfo->key, h);
		chst->erasePrefix(nfo->key);
		chst->commit();
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
	serialize_value(nfo->value, doc.revision, seqid, doc.payload);

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

bool DatabaseCore::storeToHistory(Handle h, const RawDocument &doc) {
	PInfo nfo = getDatabaseState(h);
	if (nfo == nullptr) return false;

	PChangeset chng = beginBatch(nfo);

	key_doc_revs(nfo->key, h,doc.docId, doc.revision);
	serialize_value(nfo->value,doc.revision, doc.seq_number,doc.payload);
	chng->put(nfo->key, nfo->value);

	endBatch(nfo);
	return true;

}

void DatabaseCore::endBatch(Handle h) {
	PInfo nfo = getDatabaseState(h);
	if (nfo == nullptr) return ;
	endBatch(nfo);
}

bool DatabaseCore::findDoc(Handle h, const std::string_view& docid, RawDocument& content, std::string &storage) {
	std::string key;
	key_docs(key, h, docid);
	if (!maindb->lookup(key,storage)) return false;
	value2document(storage, content);
	content.docId = docid;
	return true;
}

bool DatabaseCore::findDoc(Handle h, const std::string_view& docid, RevID revid,
		RawDocument& content, std::string &storage) {
	std::string key;
	key_docs(key, h, docid);
	if (!maindb->lookup(key,storage)) return false;
	value2document(storage, content);
	content.docId = docid;
	if (content.revision != revid) {
		key_doc_revs(key,h,docid,revid);
		if (!maindb->lookup(key,storage)) return false;
	}
	value2document(storage, content);
	return true;
}

bool DatabaseCore::enumAllRevisions(Handle h, const std::string_view& docid,
		std::function<void(const RawDocument&)> callback) {

	std::string key;
	RawDocument docinfo;
	docinfo.docId = docid;
	if (!findDoc(h, docid, docinfo, key)) return false;
	callback(docinfo);
	key_doc_revs(key, h, docid);
	Iterator iter(maindb->findRange(key));
	while (iter.getNext()) {
		value2document(iter->second, docinfo);
		callback(docinfo);
	}
	return true;
}

void DatabaseCore::eraseDoc(Handle h, const std::string_view& docid, KeySet &modifiedKeys) {
	PInfo nfo = getDatabaseState(h);
	if (nfo == nullptr) return ;

	PChangeset chng = beginBatch(nfo);
	enumAllRevisions(h, docid,[&](const RawDocument &docinf) {
		key_doc_revs(nfo->key, h, docid, docinf.revision);
		chng->erase(nfo->key);
		key_seq(nfo->key, h, docinf.seq_number);
		chng->erase(nfo->key);
	});
	key_docs(nfo->key,h, docid);
	chng->erase(nfo->key);

	for (auto &&v : nfo->viewState) {
		view_updateDocument(h, v.first, docid, std::basic_string_view<ViewUpdateRow>(), modifiedKeys);
	}

	endBatch(nfo);
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
		 bool reversed, std::function<void(const RawDocument&)> callback) {

	RawDocument dinfo;
	std::string key;
	key_docs(key,h,prefix);
	Iterator iter(maindb->findRange(key, reversed));
	auto skip = key.length() - prefix.length();
	while (iter.getNext()) {
		value2document(iter->second, dinfo);
		extract_from_key(iter->first, skip, dinfo.docId);
		callback(dinfo);
	}

}

void DatabaseCore::enumDocs(Handle h, const std::string_view& start_include,
		const std::string_view& end_exclude,
		std::function<void(const RawDocument&)> callback) {

	RawDocument dinfo;
	std::string key1, key2;
	key_docs(key1,h,start_include);
	key_docs(key2,h,end_exclude);
	Iterator iter(maindb->findRange(key1, key2));
	auto skip = key1.length() - start_include.length();
	while (iter.getNext()) {
		value2document(iter->second, dinfo);
		extract_from_key(iter->first, skip, dinfo.docId);
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

SeqNum DatabaseCore::readChanges(Handle h, SeqNum from, bool reversed,
		std::function<bool(const DocID&, const SeqNum&)>&& fn)  {
	std::string key1, key2;
	int adj = reversed?0:1;
	key_seq(key1, h, from+adj);
	key_seq(key2, h);
	std::size_t skip = key2.length();
	key_seq(key2, h+adj, 0);
	Iterator iter(maindb->findRange(key1, key2));

	DocID docId;
	SeqNum seq;

	while (iter.getNext()) {
		extract_value(iter->second,docId);
		extract_from_key(iter->first, skip, seq);
		if (!fn(docId, from)) return from;
	}
	return from;
}

bool DatabaseCore::viewLookup(ViewID viewID, const std::string_view& prefix,
		bool reversed, std::function<bool(const ViewResult&)>&& callback) {

	std::string key, value;
	std::size_t skip;
	key_view_map(key,viewID);
	skip = key.length();
	key_view_map(key,viewID, prefix);
	Iterator iter(maindb->findRange(key,reversed));
	bool f = false;
	ViewResult res;
	while (iter.getNext()) {
		extract_from_key(iter->first, skip, res.key, res.docid);
		extract_value(iter->second, res.value);
		if (!callback(res)) return true;
		f = true;
	}
	return f;

}

bool DatabaseCore::viewLookup(ViewID viewID, const std::string_view& start_key,
		const std::string_view& end_key, const std::string_view& start_doc,
		const std::string_view& end_doc,
		std::function<bool(const ViewResult&)>&& callback) {

	std::string key1, key2, value;
	std::size_t skip;
	key_view_map(key1,viewID);
	skip = key1.length();
	key_view_map(key1,viewID, start_key, start_doc);
	key_view_map(key2,viewID, end_key, end_doc);
	Iterator iter(maindb->findRange(key1,key2));
	bool f = false;
	ViewResult res;
	while (iter.getNext()) {
		extract_from_key(iter->first, skip, res.key, res.docid);
		extract_value(iter->second, res.value);
		if (!callback(res)) return true;
		f = true;
	}
	return f;
}

SeqNum DatabaseCore::needViewUpdate(Handle h, ViewID view) {
	PInfo nfo = getDatabaseState(h);
	if (nfo == nullptr) return 0;
	auto iter = nfo->viewState.find(view);
	if (iter == nfo->viewState.end()) return 0;
	if (nfo->nextSeqNum-1 == iter->second.seqNum) return 0;
	return iter->second.seqNum;
}

bool DatabaseCore::updateViewState(Handle h, ViewID view, SeqNum seqNum) {
	PInfo nfo = getDatabaseState(h);
	if (nfo == nullptr) return false;
	auto iter = nfo->viewState.find(view);
	if (iter == nfo->viewState.end()) return false;
	iter->second.seqNum = seqNum;
	PChangeset chng = beginBatch(nfo);
	key_view_state(nfo->key,h,view);
	serialize_value(nfo->value,iter->second.seqNum, iter->second.name);
	chng->put(nfo->key, nfo->value);
	endBatch(nfo);
	return true;
}

bool DatabaseCore::view_updateDocument(Handle h, ViewID view,
		const std::string_view& docId,
		const std::basic_string_view<ViewUpdateRow>& updates,
		KeySet &modifiedKeys) {

	std::string key;
	std::string value;
	key_view_docs(key, view, docId);
	PInfo nfo = getDatabaseState(h);
	if (nfo == nullptr) return false;
	if (maindb->lookup(key,value)) {
		PChangeset chng = beginBatch(nfo);
		KCursor kcrs;
		std::string_view kk;
		std::string_view vv(value);
		while (!vv.empty()) {
			extract_value(vv,kk,kcrs);
			vv = kcrs(vv);
			key_view_map(nfo->key,view,kk,docId);
			chng->erase(nfo->key);
			modifiedKeys.insert(std::string(kk));
		}
		chng->erase(key);
		endBatch(nfo);
	}
	if (!updates.empty()) {
		PChangeset chng = beginBatch(nfo);
		nfo->value2.clear();
		for (auto &&c: updates) {
			modifiedKeys.insert(std::string(c.key));
			key_view_map(nfo->key,view,c.key,docId);
			chng->put(nfo->key, c.value);
			nfo->value2.append(c.key);
			_misc::addSep(nfo->value2);
		}
		key_view_docs(nfo->key,view, docId);
		chng->put(nfo->key,nfo->value2);
		endBatch(nfo);
	}
	return true;
}

void DatabaseCore::endBatch(PInfo &nfo) {
	if (nfo->writeState.lockCount > 0) --nfo->writeState.lockCount;
	flushWriteState(nfo->writeState);
}


bool DatabaseCore::onBatchClose(Handle h, Callback &&cb) {
	PInfo nfo = getDatabaseState(h);
	if (nfo == nullptr) return false;
	if (nfo->writeState.lockCount == 0) cb();
	else nfo->writeState.waiting.push(std::move(cb));
	return true;
}

void DatabaseCore::value2document(const std::string_view &value, RawDocument &doc) {
	KCursor kc;
	extract_value(value,doc.revision, doc.seq_number, kc);
	doc.payload = kc(value);
}

void DatabaseCore::view_endUpdate(Handle h, ViewID view) {
	auto dbf = getDatabaseState(h);
	if (dbf == nullptr) return ;
	auto itr = dbf->viewState.find(view);
	if (itr == dbf->viewState.end()) return;
	itr->second.updating = false;
	while (!itr->second.waiting.empty() && !itr->second.updating) {
		auto &&fn = std::move(itr->second.waiting.front());
		itr->second.waiting.pop();
		fn();
	}
}

bool DatabaseCore::view_onUpdateFinish(Handle h, ViewID view, Callback&& cb) {
	auto dbf = getDatabaseState(h);
	if (dbf == nullptr) return false;
	auto itr = dbf->viewState.find(view);
	if (itr == dbf->viewState.end()) return false;

	if (itr->second.updating) {
		itr->second.waiting.push(std::move(cb));
	} else {
		cb();
	}
	return true;
}




bool DatabaseCore::deleteView(Handle h, ViewID view) {
	auto dbf = getDatabaseState(h);
	if (dbf == nullptr) return false;
	auto itr = dbf->viewState.find(view);
	if (itr == dbf->viewState.end()) return false;
	dbf->viewNameToID.erase(itr->second.name);
	dbf->viewState.erase(itr);
	deleteViewRaw(h,view);
	return true;
}

void DatabaseCore::deleteViewRaw(Handle h, ViewID view) {
	PChangeset chset = maindb->createChangeset();
	std::string key;
	key_view_docs(key,view);
	chset->erasePrefix(key);
	key_view_map(key,view);
	chset->erasePrefix(key);
	key_view_state(key,h,view);
	chset->erase(key);

}

ViewID DatabaseCore::allocView() {
	std::lock_guard<std::recursive_mutex> _(lock);
	return nextViewID;
}

bool DatabaseCore::view_beginUpdate(Handle h, ViewID view) {
	auto dbf = getDatabaseState(h);
	if (dbf == nullptr) return false;
	auto itr = dbf->viewState.find(view);
	if (itr == dbf->viewState.end()) return false;
	if (itr->second.updating) return false;
	itr->second.updating = true;
	return true;
}

ViewID DatabaseCore::createView(Handle h, const std::string_view& name) {
	auto dbf = getDatabaseState(h);
	if (dbf == nullptr) return 0;
	ViewID w = allocView();
	dbf->viewState[w].name = name;
	dbf->viewNameToID[std::string(name)] = w;
	auto chng = beginBatch(dbf);
	key_view_state(dbf->key, h, w);
	serialize_value(dbf->value, std::uint64_t(0), name);
	endBatch(dbf);
	return w;
}

ViewID DatabaseCore::findView(Handle h, const std::string_view& name) {
	auto dbf = getDatabaseState(h);
	if (dbf == nullptr) return 0;
	auto iter = dbf->viewNameToID.find(name);
	if (iter == dbf->viewNameToID.end()) return 0;
	return iter->second;
}


} /* namespace sofadb */
