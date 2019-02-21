/*
 * database.cpp
 *
 *  Created on: 5. 2. 2019
 *      Author: ondra
 */

#include <imtjson/array.h>
#include <imtjson/object.h>
#include <libsofa/databasecore.h>
#include <shared/logOutput.h>
#include "keyformat.h"
#include "kvapi_leveldb.h"

namespace sofadb {

DatabaseCore::DatabaseCore(PKeyValueDatabase db):maindb(db) {

	idmap.clear();
	dblist.clear();

	loadDBs();
	loadViews();
}

SeqNum DatabaseCore::getSeqNumFromDB(const std::string_view &prefix) {
	SeqNum sq;
	Iterator maxseq (maindb->findRange(prefix, true));

	if (maxseq.getNext()) {
		extract_from_key(maxseq->first,prefix.length(),sq);
	} else {
		sq=0;
	}
	return sq+1;

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
		if (dblist[c.second] != nullptr) {
			ondra_shared::logError("Can't load database $1 - handle conflict", c.first);
			continue;
		}
		auto nfo = std::make_unique<Info>();
		nfo->name = c.first;
		idmap[nfo->name] = c.second;



		key_seq(key, c.second);
		nfo->nextSeqNum = getSeqNumFromDB(key);
		key_object_index(key, c.second);
		nfo->nextHistSeqNum = getSeqNumFromDB(key);

		loadDBConfig(c.second,nfo->cfg);

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

	std::string key,value;

	auto xf = idmap.find(name);
	if (xf != idmap.end()) return xf->second;
	if (name.empty()) return invalid_handle;

	Handle h = allocSlot();
	Info &nfo = *dblist[h];
	nfo.name = name;
	nfo.nextSeqNum = 1;
	idmap.insert(std::pair<std::string_view, Handle>(nfo.name,h));

	key_db_map(key,name);
	serialize_value(value,h);

	PChangeset chst = maindb->createChangeset();
	chst->put(key,value);
	chst->commit();

	if (observer) observer(event_create,h,0);

	return h;
}

DatabaseCore::Handle DatabaseCore::getHandle(const std::string_view& name) const {

	std::lock_guard<std::recursive_mutex> _(lock);

	auto f = idmap.find(name);
	if (f == idmap.end()) return invalid_handle;
	else return f->second;
}

bool DatabaseCore::erase(Handle h) {

	PInfo nfo = getDatabaseState(h);
	if (nfo == nullptr) return false;

	idmap.erase(nfo->name);

	if (observer) observer(event_close, h,nfo->nextSeqNum);

	onBatchClose(h,[h,this]() {

		std::string key,value;

		std::unique_ptr<Info> nfo (std::move(dblist[h]));
		if (nfo == nullptr) return;

		for (auto &&w: nfo->viewState) {
			deleteViewRaw(h, w.first);
		}

		PChangeset chst = maindb->createChangeset();


		key_db_map(key, nfo->name);
		chst->erase(key);
		key_docs(key,h);
		chst->erasePrefix(key);
		key_doc_revs(key, h);
		chst->erasePrefix(key);
		key_dbconfig(key, h);
		chst->erasePrefix(key);
		key_seq(key, h);
		chst->erasePrefix(key);
		key_object_index(key, h);
		chst->erasePrefix(key);
		chst->commit();
		flushWriteState(nfo->writeState);
	});


	return true;

}

DatabaseCore::PInfo DatabaseCore::getDatabaseState(Handle h) {
	std::lock_guard<std::recursive_mutex> _(lock);
	if (h >= dblist.size()) return nullptr;
	else return PInfo(dblist[h].get());
}

void DatabaseCore::flushWriteState(WriteState &st) {
	st.curBatch->commit();
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

	std::string key,value, key2, value2;

	//generate new sequence id
	auto seqid = nfo->nextSeqNum++;

	document2value(value,  doc, seqid);

	//prepare key (contains docid)
	key_docs(key,h,doc.docId);

	//if there is already previous revision
	//we need to put it to the historical revision index
	//this reduces performance by need to load and then store the whole document
	//but the previous revision should be already in cache, because
	//we needed it to create RawDocument
	//and having current revision indexed by docid only increases
	//lookup performance

	//so try to get current revision
	if (maindb->lookup(key, value2)) {
		RawDocument curdoc;
		value2document(value2, curdoc);
		//check: if revisions are same, this is error, stop here
		if (curdoc.revision == doc.revision) {
			endBatch(h);
			return false;
		}
		//erase current seq_number slot
		key_seq(key2,h,curdoc.seq_number);
		chng->erase(key2);

		curdoc.docId = doc.docId;
		//copy revision to history
		storeToHistory(nfo, h, curdoc);
	}
	//now put the document to the storage
	chng->put(key,value);
	//generate key for sequence
	key_seq(key, h, seqid);
	//serialize document ID
	serialize_value(value,doc.docId);
	//put it to database
	chng->put(key,value);
	//all done
	endBatch(nfo);

	if (observer) observer(event_update, h, seqid);

	return true;
}

void DatabaseCore::storeToHistory(PInfo dbf, Handle h, const RawDocument &doc) {
	std::string key,value;

	PChangeset chng = beginBatch(dbf);

	SeqNum sq = dbf->nextHistSeqNum++;
	key_doc_revs(key, h, doc.docId, doc.revision);
	if (maindb->lookup(key, value)) {
		Timestamp tm;
		SeqNum oldsq;
		extract_value(value,  oldsq, tm);
		key_object_index(value,h,oldsq);
		chng->erase(value);
	}
	serialize_value(value,sq,doc.timestamp);
	chng->put(key, value);
	key_object_index(key,h,sq);
	document2value(value,doc,doc.seq_number);
	chng->put(key,value);
	endBatch(dbf);
}


bool DatabaseCore::storeToHistory(Handle h, const RawDocument &doc) {
	PInfo nfo = getDatabaseState(h);
	if (nfo == nullptr) return false;

	storeToHistory(nfo,h,doc);

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

bool DatabaseCore::findDoc(Handle h, const std::string_view& docid, RevID revid, RawDocument& content, std::string &storage) {
	std::string key;
	key_docs(key, h, docid);
	if (!maindb->lookup(key,storage)) return false;
	value2document(storage, content);
	content.docId = docid;
	if (content.revision != revid) {
		key_doc_revs(key,h,docid,revid);
		if (!maindb->lookup(key,storage)) return false;
		SeqNum sq;
		extract_value(storage, sq);
		key_object_index(key, h, sq);
		if (!maindb->lookup(key, storage)) return false;
	}
	value2document(storage, content);
	return true;
}

bool DatabaseCore::enumAllRevisions(Handle h, const std::string_view& docid,
		std::function<void(const RawDocument&)> callback) {

	std::string key,value;
	RawDocument docinfo;
	docinfo.docId = docid;
	if (!findDoc(h, docid, docinfo, key)) return false;
	callback(docinfo);
	key_doc_revs(key, h, docid);
	Iterator iter(maindb->findRange(key));
	while (iter.getNext()) {
		SeqNum sq;
		extract_value(iter->second, sq);
		key_object_index(key, h ,sq);
		if (maindb->lookup(key, value)) {
			value2document(value, docinfo);
			callback(docinfo);
		}
	}
	return true;
}

void DatabaseCore::eraseDoc(Handle h, const std::string_view& docid, KeySet &modifiedKeys) {
	PInfo nfo = getDatabaseState(h);
	if (nfo == nullptr) return ;

	std::string key;


	PChangeset chng = beginBatch(nfo);
	enumAllRevisions(h, docid,[&](const RawDocument &docinf) {
		key_doc_revs(key, h, docid, docinf.revision);
		chng->erase(key);
		key_seq(key, h, docinf.seq_number);
		chng->erase(key);
	});
	key_docs(key,h, docid);
	chng->erase(key);

	for (auto &&v : nfo->viewState) {
		view_updateDocument(h, v.first, docid, std::basic_string_view<ViewUpdateRow>(), modifiedKeys);
	}

	endBatch(nfo);

}

void DatabaseCore::eraseHistoricalDoc(Handle h, const std::string_view& docid, RevID revision) {
	PInfo nfo = getDatabaseState(h);
	if (nfo == nullptr) return ;


	std::string key,value;

	key_doc_revs(key, h, docid, revision);
	if (maindb->lookup(key,value)) {
		PChangeset chng = beginBatch(nfo);
		SeqNum oldsq;
		extract_value(value, oldsq);
		key_object_index(value,h,oldsq);
		chng->erase(value);
		chng->erase(key);
		endBatch(nfo);
	}


}

bool DatabaseCore::enumDocs(Handle h, const std::string_view& prefix,
		 bool reversed, std::function<bool(const RawDocument&)> callback) {

	RawDocument dinfo;
	std::string key;
	key_docs(key,h,prefix);
	Iterator iter(maindb->findRange(key, reversed));
	auto skip = key.length() - prefix.length();
	while (iter.getNext()) {
		value2document(iter->second, dinfo);
		extract_from_key(iter->first, skip, dinfo.docId);
		if (!callback(dinfo)) return false;
	}
	return true;

}

bool DatabaseCore::enumDocs(Handle h, const std::string_view& start_include,
		const std::string_view& end_exclude,
		std::function<bool(const RawDocument&)> callback) {

	RawDocument dinfo;
	std::string key1, key2;
	key_docs(key1,h,start_include);
	key_docs(key2,h,end_exclude);
	Iterator iter(maindb->findRange(key1, key2));
	auto skip = key1.length() - start_include.length();
	while (iter.getNext()) {
		value2document(iter->second, dinfo);
		extract_from_key(iter->first, skip, dinfo.docId);
		if (!callback(dinfo)) return false;
	}
	return true;

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
		if (!fn(docId, seq)) return seq;
	}
	return seq;
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
	std::string key,value;

	PInfo nfo = getDatabaseState(h);
	if (nfo == nullptr) return false;
	auto iter = nfo->viewState.find(view);
	if (iter == nfo->viewState.end()) return false;
	iter->second.seqNum = seqNum;
	PChangeset chng = beginBatch(nfo);
	key_view_state(key,h,view);
	serialize_value(value,iter->second.seqNum, iter->second.name);
	chng->put(key, value);
	endBatch(nfo);
	return true;
}

bool DatabaseCore::view_updateDocument(Handle h, ViewID view,
		const std::string_view& docId,
		const std::basic_string_view<ViewUpdateRow>& updates,
		KeySet &modifiedKeys) {

	std::string key;
	std::string value;
	std::string key2,value2;

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
			key_view_map(key,view,kk,docId);
			chng->erase(key);
			modifiedKeys.insert(std::string(kk));
		}
		chng->erase(key);
		endBatch(nfo);
	}
	if (!updates.empty()) {
		PChangeset chng = beginBatch(nfo);
		value2.clear();
		for (auto &&c: updates) {
			modifiedKeys.insert(std::string(c.key));
			key_view_map(key,view,c.key,docId);
			chng->put(key, c.value);
			value2.append(c.key);
			_misc::addSep(value2);
		}
		key_view_docs(key,view, docId);
		chng->put(key,value2);
		endBatch(nfo);
	}
	return true;
}

void DatabaseCore::endBatch(PInfo &nfo) {
	if (nfo->writeState.lockCount > 0) {
		--nfo->writeState.lockCount;
		if (nfo->writeState.lockCount == 0) {
			flushWriteState(nfo->writeState);
		}
	}
}


bool DatabaseCore::onBatchClose(Handle h, Callback &&cb) {
	PInfo nfo = getDatabaseState(h);
	if (nfo == nullptr) return false;
	if (nfo->writeState.lockCount == 0) cb();
	else nfo->writeState.waiting.push(std::move(cb));
	return true;
}

void DatabaseCore::document2value(std::string& value, const RawDocument& doc, SeqNum seqid) {
	serialize_value(value, doc.revision, seqid, doc.timestamp,
			static_cast<unsigned char>(doc.version | (doc.deleted ? 0x80 : 0)), doc.payload);
}


void DatabaseCore::value2document(const std::string_view &value, RawDocument &doc) {
	KCursor kc;
	unsigned char ver_del;
	extract_value(value,doc.revision, doc.seq_number, doc.timestamp, ver_del, kc);
	doc.version = ver_del & 0x7F;
	doc.deleted = (ver_del & 0x80) != 0;
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
	std::string key,value;

	auto dbf = getDatabaseState(h);
	if (dbf == nullptr) return 0;
	ViewID w = allocView();
	dbf->viewState[w].name = name;
	dbf->viewNameToID[std::string(name)] = w;
	auto chng = beginBatch(dbf);
	key_view_state(key, h, w);
	serialize_value(value, std::uint64_t(0), name);
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

bool DatabaseCore::rename(Handle h, const std::string_view& newname) {
	if (newname.empty()) return false;

	std::string key,key2,value;


	auto dbf = getDatabaseState(h);
	std::lock_guard<std::recursive_mutex> _(lock);
	auto newn = idmap.find(newname);
	if (newn == idmap.end()) {
		idmap.erase(dbf->name);
		key_db_map(key, dbf->name);
		dbf->name = newname;
		key_db_map(key2, dbf->name);
		idmap.insert(std::pair<std::string_view,Handle>(dbf->name, h));

		serialize_value(value,h);

		PChangeset chst = maindb->createChangeset();
		chst->erase(key);
		chst->put(key2,value);
		chst->commit();
		return true;
	} else {
		return false;
	}
}

void DatabaseCore::setObserver(Observer&& observer) {
	std::lock_guard<std::recursive_mutex> _(lock);
	this->observer = std::move(observer);
	Handle h = 0;
	for (auto &&c: dblist) {
		if (c != nullptr) this->observer(event_create,h,c->nextSeqNum-1);
		++h;
	}
}

void json2dbconfig(json::Value data, DatabaseCore::DBConfig &cfg) {
	json::Value v = data["history_max_age"];
	if (v.defined()) cfg.history_max_age = v.getUInt();
	v = data["history_max_count"];
	if (v.defined()) cfg.history_max_count = v.getUInt();
	v = data["history_max_deleted"];
	if (v.defined()) cfg.history_max_deleted = v.getUInt();
	v = data["history_min_count"];
	if (v.defined()) cfg.history_min_count = v.getUInt();
	v = data["logsize"];
	if (v.defined()) cfg.logsize = v.getUInt();
}

bool DatabaseCore::loadDBConfig(Handle h, DBConfig &cfg) {
	std::string key,value;
	key_dbconfig(key,h,"config");
	if (maindb->lookup(key,value)) {
		std::string_view src(value);
		json::Value data = json::Value::parseBinary(JsonSource(src),json::utf8encoding);
		json2dbconfig(data,cfg);
		return true;
	} else {
		return false;
	}
}

json::Value dbconfig2json(const DatabaseCore::DBConfig &cfg) {
	json::Object obj;
	obj.set("history_max_age", cfg.history_max_age)
		   ("history_max_count",cfg.history_max_count)
		   ("history_max_deleted",cfg.history_max_deleted)
		   ("history_min_count",cfg.history_min_count)
		   ("logsize",cfg.logsize);

	return obj;
}

bool DatabaseCore::storeDBConfig(Handle h, const DBConfig &cfg) {

	std::string key,value;

	PInfo dbf = getDatabaseState(h);
	if (dbf == nullptr) return false;

	json::Value data = dbconfig2json(cfg);
	value.clear();
	data.serializeBinary(JsonTarget(value), json::compressKeys);
	key_dbconfig(key,h,"config");
	PChangeset chs = beginBatch(dbf);
	chs->put(key, value);
	dbf->cfg = cfg;
	endBatch(dbf);
	return true;
}

bool DatabaseCore::getConfig(Handle h, DBConfig &cfg)  {
	PInfo dbf = getDatabaseState(h);
	if (dbf == nullptr) return false;
	cfg = dbf->cfg;
	return true;
}

std::size_t DatabaseCore::getMaxLogSize(Handle h)  {
	PInfo dbf = getDatabaseState(h);
	if (dbf == nullptr) return 0;
	return dbf->cfg.logsize;
}

bool DatabaseCore::setConfig(Handle h, const DBConfig &cfg) {
	return storeDBConfig(h, cfg);
}

struct HistStat {
	Timestamp tm;
	RevID rev;
	SeqNum sq;
};

bool DatabaseCore::cleanHistory(Handle h, const std::string_view &docid) {

	RawDocument topdoc;
	std::string key;
	if (!findDoc(h,docid,topdoc, key)) return false;

	DBConfig cfg;
	getConfig(h,cfg);

	key_doc_revs(key, h, docid);
	Iterator iter(maindb->findRange(key));

	std::vector<HistStat> hs;
	std::vector<RevID> todel;

	while (iter.getNext()) {
		HistStat h;
		extract_from_key(iter->first, key.length(), h.rev);
		extract_value(iter->second, h.sq, h.tm);
		hs.push_back(h);
	}

	auto sortfn = [](const HistStat &a, const HistStat &b) {
		return a.tm > b.tm;
	};

	std::sort(hs.begin(), hs.end(),sortfn);

	std::size_t now = std::chrono::duration_cast< std::chrono::milliseconds >(std::chrono::system_clock::now().time_since_epoch()).count();
	std::size_t old = now-cfg.history_max_age;
	while (hs.size() > cfg.history_min_count && !hs.empty() && hs.back().tm < old) {
		todel.push_back(hs.back().rev);
		hs.pop_back();
	}

	if (topdoc.deleted) {
		while (hs.size() > cfg.history_max_deleted) {
			todel.push_back(hs.back().rev);
			hs.pop_back();
		}
	}

	/*if (rawdocs.size() > cfg.history_max_count) {

	}*/

	beginBatch(h,false);
	try {
		for (auto &&c: todel) {
			eraseHistoricalDoc(h,docid,c);
		}
		endBatch(h);
	} catch (...) {
		endBatch(h);
		throw;
	}
	return true;
}



} /* namespace sofadb */
