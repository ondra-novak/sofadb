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
#include "kvapi_memdb.h"

namespace sofadb {

DatabaseCore::DatabaseCore(PKeyValueDatabase db):maindb(db) {

	memdb = new MemDB;

	idmap.clear();
	dblist.clear();

	loadDBs();
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
	Iterator iter (maindb->findRange(key));
	dblist.clear();
	loadDB(iter);
}


DatabaseCore::Handle DatabaseCore::allocSlot() {
	std::size_t cnt = dblist.size();
	for (std::size_t i = 0; i < cnt; i++) {
		if (dblist[i] == nullptr) {
			return i;
		}
	}
	dblist.resize(cnt+1);
	return cnt;
}

DatabaseCore::Handle DatabaseCore::create(const std::string_view& name, Storage storage) {

	std::lock_guard<std::recursive_mutex> _(lock);

	std::string key,value;

	auto xf = idmap.find(name);
	if (xf != idmap.end()) return xf->second;
	if (name.empty()) return invalid_handle;

	Handle h = allocSlot();

	if (storage == Storage::memory) h |= memdb_mask;

	key_db_map(key,h);
	serialize_value(value,name);

	PChangeset chst = maindb->createChangeset();
	chst->put(key,value);
	chst->commit();

	Iterator iter(maindb->findRange(key,false));
	loadDB(iter);


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

		PChangeset chst = selectDB(h)->createChangeset();


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
		key_view_docs(key, h);
		chst->erasePrefix(key);
		key_view_map(key, h);
		chst->erasePrefix(key);
		key_view_state(key, h);
		chst->erasePrefix(key);
		chst->commit();

		chst = maindb->createChangeset();
		key_db_map(key, h);
		chst->erase(key);
		chst->commit();

		flushWriteState(nfo->writeState);
	});



	return true;

}

DatabaseCore::PInfo DatabaseCore::getDatabaseState(Handle h) {
	std::lock_guard<std::recursive_mutex> _(lock);
	h &= index_mask;
	if (h >= dblist.size()) return nullptr;
	else return PInfo(dblist[h].get());
}

void DatabaseCore::flushWriteState(WriteState &st) {
	if (st.curBatch != nullptr) st.curBatch->commit();
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

PChangeset DatabaseCore::beginBatch(const PInfo &nfo) {
	++nfo->writeState.lockCount;
	if (nfo->writeState.curBatch == nullptr)
		nfo->writeState.curBatch = selectDB(nfo->storage)->createChangeset();
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
	if (selectDB(h)->lookup(key, value2)) {
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
	serialize_value(value,doc.revision,doc.docId);
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
	if (selectDB(h)->lookup(key, value)) {
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
	if (!selectDB(h)->lookup(key,storage)) return false;
	value2document(storage, content);
	content.docId = docid;
	return true;
}

bool DatabaseCore::findDoc(Handle h, const std::string_view& docid, RevID revid, RawDocument& content, std::string &storage) {
	std::string key;
	key_docs(key, h, docid);
	PKeyValueDatabase db = selectDB(h);
	if (!db->lookup(key,storage)) return false;
	value2document(storage, content);
	content.docId = docid;
	if (content.revision != revid) {
		key_doc_revs(key,h,docid,revid);
		if (!db->lookup(key,storage)) return false;
		SeqNum sq;
		extract_value(storage, sq);
		key_object_index(key, h, sq);
		if (!db->lookup(key, storage)) return false;
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
	auto db = selectDB(h);
	Iterator iter(db->findRange(key));
	while (iter.getNext()) {
		SeqNum sq;
		extract_value(iter->second, sq);
		key_object_index(key, h ,sq);
		if (db->lookup(key, value)) {
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

/*	for (auto &&v : nfo->viewState) {
		view_updateDocument(h, v.first, docid, std::basic_string_view<ViewUpdateRow>(), modifiedKeys);
	}*/

	endBatch(nfo);

}

void DatabaseCore::eraseHistoricalDoc(Handle h, const std::string_view& docid, RevID revision) {
	PInfo nfo = getDatabaseState(h);
	if (nfo == nullptr) return ;


	std::string key,value;

	key_doc_revs(key, h, docid, revision);
	if (selectDB(h)->lookup(key,value)) {
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
	Iterator iter(selectDB(h)->findRange(key, reversed));
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
	Iterator iter(selectDB(h)->findRange(key1, key2));
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
	if (!selectDB(h)->lookup(key, value)) return false;
	extract_value(docid);
	return true;

}

SeqNum DatabaseCore::readChanges(Handle h, SeqNum from, bool reversed,
		std::function<bool(const ChangeRec &)>&& fn)  {
	std::string key1, key2;
	int adj = reversed?0:1;
	key_seq(key1, h, from+adj);
	key_seq(key2, h);
	std::size_t skip = key2.length();
	key_seq(key2, h+adj, 0);
	Iterator iter(selectDB(h)->findRange(key1, key2));

	std::string_view docId;
	SeqNum seq = from;
	RevID revid;

	while (iter.getNext()) {
		extract_value(iter->second,revid,docId);
		extract_from_key(iter->first, skip, seq);
		if (!fn(ChangeRec({docId,revid,seq}))) return seq;
	}
	return seq;
}


void DatabaseCore::endBatch(const PInfo &nfo) {
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


bool DatabaseCore::rename(Handle h, const std::string_view& newname) {
	if (newname.empty()) return false;

	std::string key,key2,value;


	auto dbf = getDatabaseState(h);
	std::lock_guard<std::recursive_mutex> _(lock);
	auto newn = idmap.find(newname);
	if (newn == idmap.end()) {

		key_db_map(key, h);
		serialize_value(value, newname);

		idmap.erase(dbf->name);
		idmap.insert(std::pair<std::string_view,Handle>(dbf->name, h));

		PChangeset chst = maindb->createChangeset();
		chst->put(key,value);
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
		if (c != nullptr) this->observer(event_create,
				(c->storage == Storage::memory?memdb_mask:0)|h,
				c->nextSeqNum-1);
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
	PChangeset chs = maindb->createChangeset();
	chs->put(key, value);
	dbf->cfg = cfg;
	chs->commit();
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

bool DatabaseCore::cleanHistory(Handle h, const std::string_view &docid, const RevMap &revision_map) {

	RawDocument topdoc;
	std::string key;
	if (!findDoc(h,docid,topdoc, key)) return false;

	DBConfig cfg;
	getConfig(h,cfg);

	key_doc_revs(key, h, docid);
	auto db = selectDB(h);
	Iterator iter(db->findRange(key));

	std::vector<HistStat> hs;
	std::vector<RevID> todel;

	while (iter.getNext()) {
		HistStat h;
		extract_from_key(iter->first, key.length()+2, h.rev);
		auto iter2 = revision_map.find(h.rev);
		if (iter2 == revision_map.end()) {
			todel.push_back(h.rev);
		} else {
			extract_value(iter->second, h.sq, h.tm);
			if (iter2->second == false) {
				hs.push_back(h);
			}
		}
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

	if (hs.size() > cfg.history_max_count && hs.size() > 2) {
		std::size_t cnt = cfg.history_max_count;
		if (cnt <=2) {
			for (std::size_t i = 1, c = hs.size()-1; i < c; i++) {
				todel.push_back(hs[i].rev);
			}
		} else {
			while (hs.size() > cnt) {
				std::size_t maxdist=static_cast<std::size_t>(-1);
				std::size_t maxdistidx = 0;
				for (std::size_t i = 1,c = hs.size()-1; i < c ; i++) {
					std::size_t dist1 =  hs[i-1].tm - hs[i].tm;
					std::size_t dist2 =  hs[i].tm - hs[i+1].tm;
					std::size_t dist = std::min(dist1,dist2);
					if (dist < maxdist) {
						maxdist = dist;
						maxdistidx = i;
					}
				}
				todel.push_back(hs[maxdistidx].rev);
				hs.erase(hs.begin()+maxdistidx);
			}
		}

	}

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

PKeyValueDatabase DatabaseCore::selectDB(Handle h) const {
	if (h & memdb_mask) return memdb; else return maindb;
}
PKeyValueDatabase DatabaseCore::selectDB(Storage storage) const {
	if (storage == Storage::memory) return memdb; else return maindb;
}


void DatabaseCore::loadDB(Iterator &iter) {
	std::string key;
	while (iter.getNext()) {
		Handle h;
		std::string_view name;
		extract_from_key(iter->first, 1, h);
		extract_value(iter->second, name);
		std::size_t idx = h & index_mask;
		if (dblist.size()<=idx)
			dblist.resize(idx+1);

		auto nfo = std::make_unique<Info>();

		nfo->name = name;
		idmap[nfo->name] = h;

		if ((h & memdb_mask) == 0) {

			key_seq(key, h);
			nfo->nextSeqNum = getSeqNumFromDB(key);
			key_object_index(key, h);
			nfo->nextHistSeqNum = getSeqNumFromDB(key);
			nfo->storage = Storage::permanent;

		} else {
			nfo->nextSeqNum = 1;
			nfo->nextHistSeqNum = 1;
			nfo->storage = Storage::memory;
		}

		loadDBConfig(h,nfo->cfg);



		key_view_state(key,idx);
		Iterator vi ( maindb->findRange(key,false) );
		while (vi.getNext()) {
			std::uint32_t viewid;
			extract_from_key(vi->first, key.length(), viewid);
			ViewState st;
			extract_value(vi->second, st.seqNum, st.name);
			st.updating = false;

			while (nfo->viewState.size() <= viewid) nfo->viewState.push_back(nullptr);
			nfo->viewState[viewid] = std::make_unique<ViewState>(st);
			nfo->viewNameToID[nfo->viewState[viewid]->name] = viewid;
		}

		dblist[idx] = std::move(nfo);

		if (observer) observer(event_create,h,0);

	}
}

DatabaseCore::Lock DatabaseCore::lockWrite(Handle h) {
	return Lock(getDatabaseState(h));
}

ViewID DatabaseCore::createView(Handle h, const std::string_view &name) {

	std::string key,value;
	PInfo nfo = getDatabaseState(h);
	if (nfo == nullptr) return invalid_handle;
	auto iter = nfo->viewNameToID.find(name);
	if (iter != nfo->viewNameToID.end()) return invalid_handle;

	std::size_t id;
	for (id = 0; id < nfo->viewState.size(); id++) {
		if (nfo->viewState[id] == nullptr) break;
	}
	if (id == nfo->viewState.size()) nfo->viewState.push_back(nullptr);

	PChangeset ch = beginBatch(nfo);
	key_view_state(key,h,id);
	serialize_value(value,ViewID(0),name);
	ch->put(key,value);
	endBatch(nfo);

	ViewState vst;
	vst.name = name;
	vst.seqNum = 0;
	vst.updating = false;
	nfo->viewState[id] = std::make_unique<ViewState>(vst);
	nfo->viewNameToID[nfo->viewState[id]->name] = id;

	return id;
}

bool DatabaseCore::eraseView(Handle h, ViewID viewId) {

	PInfo nfo = getDatabaseState(h);
	if (nfo == nullptr) return false;
	if (viewId >= nfo->viewState.size()) return false;
	auto & vst = nfo->viewState[viewId];
	if (vst == nullptr) return false;

	nfo->viewNameToID.erase(nfo->viewState[viewId]->name);
	PViewState &st = nfo->viewState[viewId];

	auto clearJob = [this,h,viewId] {

		PInfo nfo = getDatabaseState(h);
		if (nfo == nullptr) return;

		PChangeset chs = this->beginBatch(nfo);
		std::string key;
		key_view_docs(key,h,viewId);
		chs->erasePrefix(key);
		key_view_map(key,h,viewId);
		chs->erasePrefix(key);
		key_view_state(key,h,viewId);
		chs->erasePrefix(key);

		this->endBatch(nfo);
		nfo->viewState[viewId] = nullptr;

	};

	if (st->updating) {
		//force stop updating
		st->seqNum = -1;
		st->waiting.push(clearJob);
	} else {
		clearJob();
	}

	return true;
}

bool DatabaseCore::view_updateDoc(Handle h, ViewID viewId, SeqNum seqNum,
		const std::string_view& docId,
		const std::basic_string_view<std::pair<std::string,std::string> >& keyvaluedata,
		AlterKeyObserver &&altered_keys) {

	PInfo nfo = getDatabaseState(h);
	if (nfo == nullptr) return false;
	ViewState *vst = nfo->getViewState(viewId);
	if (vst == nullptr) return false;
	if (vst->seqNum >= seqNum) return false;

	PChangeset ch = beginBatch(nfo);

	std::string key,value;

	view_eraseDoc2(h,viewId,nfo,docId, std::move(altered_keys));
	if (!keyvaluedata.empty()) {
		for (auto &&c : keyvaluedata) {
			key_view_map(key,h,viewId,c.first,docId);
			ch->put(key,c.second);
		}
		for (auto &&c: keyvaluedata) {
			value.append(c.first);
			value.push_back(0);
			value.push_back(0);
			altered_keys(c.first);
		}
		key_view_docs(key,h,viewId,docId);
		value.pop_back();
		value.pop_back();
		ch->put(key,value);
	}
	vst->seqNum = seqNum;
	endBatch(nfo);
	return true;


}

bool DatabaseCore::view_needUpdate(Handle h, ViewID viewId) {
	PInfo nfo = getDatabaseState(h);
	if (nfo == nullptr) return false;
	ViewState *vst = nfo->getViewState(viewId);
	if (vst == nullptr) return false;
	return vst->seqNum+1 < nfo->nextSeqNum;

}

bool DatabaseCore::view_lockUpdate(Handle h, ViewID viewId) {
	PInfo nfo = getDatabaseState(h);
	if (nfo == nullptr) return false;
	ViewState *vst = nfo->getViewState(viewId);
	if (vst == nullptr) return false;
	if (vst->seqNum+1 >= nfo->nextSeqNum) return false;
	if (vst->updating) return false;
	vst->updating = true;
	return true;
}

bool DatabaseCore::view_waitForUpdate(Handle h, ViewID viewId, Callback callback) {
	PInfo nfo = getDatabaseState(h);
	if (nfo == nullptr) return false;
	ViewState *vst = nfo->getViewState(viewId);
	if (vst == nullptr) return false;
	if (vst->updating) vst->waiting.push(callback);
	else {
		nfo = nullptr;
		callback();
	}
	return true;
}

bool DatabaseCore::view_finishUpdate(Handle h, ViewID viewId) {
	PInfo nfo = getDatabaseState(h);
	if (nfo == nullptr) return false;
	ViewState *vst = nfo->getViewState(viewId);
	if (vst == nullptr) return false;
	if (!vst->updating) return false;

	//store final sequence number into database
	PChangeset ch = beginBatch(nfo);
	std::string key, value;
	key_view_state(key,h,viewId);
	serialize_value(value,vst->seqNum,vst->name);
	ch->put(key,value);
	endBatch(nfo);

	vst->updating = false;
	decltype(vst->waiting) q;
	std::swap(q, vst->waiting);
	nfo = nullptr;
	while (!q.empty()) {
		Callback cb ( std::move(q.front()));
		q.pop();
		cb();
	}
	return true;

}

void DatabaseCore::view_eraseDoc2(Handle h, ViewID viewId, const PInfo &nfo,
		const std::string_view& doc_id, AlterKeyObserver && altered_keys) {
	PChangeset ch = beginBatch(nfo);

	std::string key;
	std::string value;
	key_view_docs(key, h, viewId, doc_id);
	if (maindb->lookup(key, value)) {
		ch->erase(key);
		json::StrViewA data(value);
		auto splt = data.split(json::StrViewA("\0\0", 2));
		while (!!splt) {
			std::string_view kv = splt();
			altered_keys(kv);
			key_view_map(key, h, viewId, kv, doc_id);
			ch->erase(key);
		}
	}
	endBatch(nfo);

}

bool DatabaseCore::view_eraseDoc(Handle h, ViewID viewId,
		std::string_view& doc_id, AlterKeyObserver&& altered_keys) {

	PInfo nfo = getDatabaseState(h);
	if (nfo == nullptr) return false;


	view_eraseDoc2(h, viewId, nfo, doc_id, std::move(altered_keys));
	return true;
}

SeqNum DatabaseCore::view_getDocKeys(Handle h, ViewID viewId,
		std::string_view& docId,
		std::function<bool(const ViewResult&)> &&callback) {

	PKeyValueDatabaseSnapshot snap = maindb->createSnapshot();
	std::string key,value,value2;
	SeqNum num = 0;
	key_view_state(key,h,viewId);
	if (snap->lookup(key,value)) {
		extract_value(value,num);
		key_view_docs(key,h,viewId,docId);
		if (snap->lookup(key,value)) {
			json::StrViewA data(value);
			auto splt = data.split(json::StrViewA("\0\0", 2));
			while (!!splt) {
				std::string_view kv = splt();
				key_view_map(key,h,viewId,kv,docId);
				snap->lookup(key, value2);
				if (!callback(ViewResult{docId,kv,value2}))
					break;
			}
		}
	}
	return num;
}

SeqNum DatabaseCore::view_list(Handle h, ViewID viewId,
		std::string_view& prefix, bool reversed,
		std::function<bool(const ViewResult&)> &&callback) {

	PKeyValueDatabaseSnapshot snap = maindb->createSnapshot();
	std::string key,value;
	SeqNum num = 0;
	key_view_state(key,h,viewId);
	if (snap->lookup(key,value)) {
		extract_value(value,num);
		key_view_map(key,h,viewId,prefix);
		Iterator iter(snap->findRange(key,reversed));
		while (iter.getNext()) {
			std::string_view kv,docId;
			extract_from_key(iter->first, key.length()-prefix.length(), kv, docId);
			if (!callback(ViewResult{docId,kv,iter->second}))
				break;
		}
	}
	return num;
}

SeqNum DatabaseCore::view_getSeqNum(Handle h, ViewID viewId) {
	PInfo nfo = getDatabaseState(h);
	if (nfo == nullptr) return false;
	ViewState *vst = nfo->getViewState(viewId);
	if (vst == nullptr) return false;
	return vst->seqNum;
}

SeqNum DatabaseCore::view_list(Handle h, ViewID viewId,
		std::string_view& start_key, std::string_view& end_key,
		std::function<bool(const ViewResult&)> &&callback) {

	PKeyValueDatabaseSnapshot snap = maindb->createSnapshot();
	std::string key1,key2,value;
	SeqNum num = 0;
	key_view_state(key1,h,viewId);
	if (snap->lookup(key1,value)) {
		extract_value(value,num);
		key_view_map(key1,h,viewId,start_key);
		key_view_map(key2,h,viewId,end_key);
		Iterator iter(snap->findRange(key1,key2));
		while (iter.getNext()) {
			std::string_view kv,docId;
			extract_from_key(iter->first, key1.length()-start_key.length(), kv, docId);
			if (!callback(ViewResult{docId,kv,iter->second}))
				break;
		}
	}
	return num;
}

bool DatabaseCore::view_update(Handle h, ViewID viewId, std::size_t limit, ViewUpdateFn &&updateFn, AlterKeyObserver &&observer) {
	if (view_lockUpdate(h,viewId)) {
		try {
			SeqNum seqnum = view_getSeqNum(h,viewId);
			std::string_view docId;
			std::string tmp;
			std::basic_string<std::pair<std::string, std::string> > kvdata;

			ViewEmitFn emitFn = [&](std::string_view key, std::string_view value) {
				kvdata.push_back(std::pair(std::string(key),std::string(value)));
			};

			readChanges(h,seqnum,false,[&](const ChangeRec &rec) {

				docId = rec.docid;
				RawDocument rawdoc;
				if (findDoc(h,docId,rawdoc,tmp)) {
					kvdata.clear();
					updateFn(rawdoc,emitFn);
					view_updateDoc(h,viewId,rec.seqnum,docId,kvdata,std::move(observer));
				}
				return --limit;
			});

			view_finishUpdate(h,viewId);
			return true;
		} catch (...) {
			view_finishUpdate(h,viewId);
			throw;
		}
	} else {
		return false;
	}
}


} /* namespace sofadb */
