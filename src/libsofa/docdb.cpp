/*
 * docdb.cpp
 *
 *  Created on: 7. 2. 2019
 *      Author: ondra
 */

#include <unordered_set>
#include <imtjson/array.h>
#include <imtjson/object.h>
#include <imtjson/binjson.tcc>
#include <libsofa/docdb.h>
#include <imtjson/fnv.h>
#include <imtjson/operations.h>
#include <imtjson/string.h>
#include <libsofa/keyformat.h>
#include <libsofa/merge.h>
#include <shared/logOutput.h>
#include "merge_logs.h"

namespace sofadb {

using namespace json;



RevID DocumentDB::parseStrRev(const std::string_view &strrev) {
	RevID rev = 0;
	for (auto &&c: strrev) {
		RevID d;
		if (std::isdigit(c)) d = c - '0';
		else if (std::isupper(c)) d = c - 'A' + 10;
		else if (std::islower(c)) d = c - 'a' + 36;
		else continue;
		rev = rev * 62 + d;
	}
	return rev;
}
std::string_view DocumentDB::serializeStrRev(RevID rev, char *out, int leftZeroes) {
	if (rev == 0 && leftZeroes <= 0) return std::string_view(out,0);
	auto r = serializeStrRev(rev/62, out, leftZeroes-1);
	char *nx = out+r.size();
	RevID d = rev % 62;
	if (d < 10) *nx = d+'0';
	else if (d < 36) *nx = d-10+'A';
	else *nx = d-36+'a';
	return std::string_view(out,r.size()+1);
}


String DocumentDB::serializeStrRev(RevID rev) {

	constexpr auto digits = 12*sizeof(rev)/8;

	return String(digits, [&](char *c) {
		auto res = serializeStrRev(rev, c,digits);
		return res.size();
	});
}


std::uint64_t DocumentDB::getTimestamp() {
	using namespace std::chrono;
	milliseconds ms = duration_cast< milliseconds >(
	    system_clock::now().time_since_epoch()
	);
	return ms.count();
}

json::Value DocumentDB::parseStrRevArr(const json::Value &arr) {
	return arr.map([&](Value x) {
		return parseStrRev(x.getString());
		});
}
json::Value DocumentDB::serializeStrRevArr(const json::Value &arr){
	return arr.map([&](Value x) {
		return serializeStrRev(x.getUInt());
		});
}


DocumentDB::DocumentDB(DatabaseCore& core):core(core) {
}

PutStatus DocumentDB::createPayload(const json::Value &doc, json::Value &payload) {

	Value data = doc["data"];
	Value conflicts = doc["conflicts"];
	if (!conflicts.defined()) conflicts = json::array;
	else if (conflicts.type() !=json::array) return PutStatus::error_conflicts_must_be_array;

	payload = {data, conflicts};
	return PutStatus::stored;
}

json::Value DocumentDB::get(Handle h, const std::string_view& id, OutputFormat oform) {
	DatabaseCore::RawDocument rdoc;
	std::string tmp;
	if (!core.findDoc(h,id,rdoc,tmp)) return nullptr;
	return parseDocument(rdoc, oform);
}

json::Value DocumentDB::get(Handle h, const std::string_view& id, const std::string_view& rev, OutputFormat oform) {
	DatabaseCore::RawDocument rdoc;
	std::string tmp;
	if (!core.findDoc(h,id,parseStrRev(rev), rdoc,tmp)) return nullptr;
	return parseDocument(rdoc, oform);
}

void DocumentDB::serializePayload(const json::Value &newhst, const json::Value &conflicts, const json::Value &payload,  std::string &tmp) {
	tmp.clear();
	newhst.stripKey().serializeBinary(JsonTarget(tmp),0);
	conflicts.stripKey().serializeBinary(JsonTarget(tmp),0);
	payload.stripKey().serializeBinary(JsonTarget(tmp),json::compressKeys);
}

PutStatus DocumentDB::json2rawdoc(const json::Value &doc, DatabaseCore::RawDocument  &rawdoc, bool new_edit) {
	Value jid = doc["id"];
	if (jid.type() != json::string) return PutStatus::error_id_must_be_string;
	Value jrev = doc["rev"];
	if (jrev.type() != json::string) {
		if (!new_edit || jrev.defined())
			return PutStatus::error_rev_must_be_string;
	}
	std::string_view id = jid.getString();
	std::uint64_t timestamp;
	if (!new_edit) {
		Value jtimestamp = doc["timestamp"];
		if (jtimestamp.type() != json::number) return PutStatus::error_timestamp_must_be_number;
		timestamp = jtimestamp.getUInt();
	} else {
		timestamp = getTimestamp();
	}
	RevID rev = parseStrRev(jrev.getString());
	Value jdel = doc["deleted"];
	if (jdel.defined() && jdel.type() != json::boolean)
		return PutStatus::error_deleted_must_be_bool;
	rawdoc.deleted = jdel.getBool();
	rawdoc.docId = id;
	rawdoc.revision = rev;
	rawdoc.timestamp = timestamp;
	rawdoc.version = 0;
	rawdoc.seq_number = 0;
	return PutStatus::stored;
}


PutStatus DocumentDB::loadDataConflictsLog(const json::Value &doc,
		Value *data, Value *conflicts, Value *log) {
	if (data) {
		*data = doc["data"];
		if (!data->defined()) return PutStatus::error_data_is_manadatory;
	}
	if (conflicts) {
		*conflicts = doc["conflicts"];
		if (conflicts->defined()) {
			if (conflicts->type() != json::array) return PutStatus::error_conflicts_must_be_array;
			for (Value v: *conflicts)
				if (v.type() != json::string) return PutStatus::error_conflict_must_be_string;
		}
	}
	if (log) {
		*log = doc["log"];
		if (!log->defined()) return PutStatus::error_log_is_mandatory;
		for (Value v: *log)
			if (v.type() != json::string) return PutStatus::error_log_item_must_be_string;
	}
	return PutStatus::stored;
}

RevID DocumentDB::calcRevisionID(json::Value data,json::Value conflicts, json::Value timestamp, json::Value deleted) {
	RevID newRev;
	Value({data, conflicts, timestamp, deleted}).serializeBinary(FNV1a<sizeof(RevID)>(newRev),0);
	return newRev;
}

json::Value DocumentDB::convertClientPut2ReplicatorPut(json::Value doc) {
	Object newdoc;
	Value data = doc["data"];
	Value id = doc["id"];
	Value conflicts = doc["conflicts"];
	Value timestamp = getTimestamp();
	Value deleted = doc["deleted"];
	RevID newRev = calcRevisionID(data,conflicts,timestamp,deleted);

	newdoc.set(doc["id"]);
	newdoc.set(doc["data"]);
	newdoc.set("rev", serializeStrRev(newRev));
	newdoc.set("log", Value(json::array, {doc["rev"]}));
	newdoc.set("timestamp", timestamp);
	newdoc.set("deleted", deleted);
	newdoc.set("conflicts", conflicts);

	return newdoc;
}


PutStatus DocumentDB::client_put(Handle h, const json::Value &doc, json::Value &outrev) {

	Value conv = convertClientPut2ReplicatorPut(doc);
	String mergerev;
	outrev = conv["rev"].toString();

	auto st = replicator_put(h, conv,mergerev);
	if (st == PutStatus::merged) {
		outrev = {outrev,  mergerev};
	}
	return st;

/*	DatabaseCore::RawDocument rawdoc;
	DatabaseCore::RawDocument prevdoc;
	std::string tmp;
	Value newhst;
	PutStatus st;
	Value data;
	Value conflicts;

	st = json2rawdoc(doc, rawdoc, true);
	if (st != PutStatus::stored) return st;

	st = loadDataConflictsLog(doc, &data, &conflicts,nullptr);
	if (st != PutStatus::stored) return st;

	RevID newRev = calcRevisionID(data,conflicts,rawdoc.timestamp,rawdoc.deleted);
	bool prevrev_ndefined = doc["rev"].getString().empty();

	bool exists = core.findDoc(h,rawdoc.docId,prevdoc, tmp);
	if (exists) {
		if (prevrev_ndefined) {
			if (prevdoc.deleted) {
				rawdoc.revision = prevdoc.revision;
			} else {
				return PutStatus::conflict;
			}
		}
		RevID revid = rawdoc.revision;
		if (rawdoc.revision != prevdoc.revision) return PutStatus::conflict;
		Value hst = Value::parseBinary(JsonSource(prevdoc.payload));
		Array joinhst;
		joinhst.reserve(hst.size()+1);
		joinhst.push_back(revid);
		joinhst.addSet(hst);
		newhst = Value(joinhst).slice(0,core.getMaxLogSize(h));
	} else {
		if (!prevrev_ndefined) return PutStatus::conflict;
		newhst = json::array;
	}
	serializePayload(newhst, conflicts, data, tmp);
	rawdoc.payload = tmp;
	rawdoc.revision = newRev;
	core.storeUpdate(h,rawdoc);
	tmp.clear();
	outrev = serializeStrRev(newRev);
	return PutStatus::stored;
	*/
}



PutStatus DocumentDB::replicator_put(Handle h, const json::Value &doc, json::String &outrev) {
	DatabaseCore::RawDocument rawdoc;
	DatabaseCore::RawDocument prevdoc;
	std::string tmp;
	Value newhst;
	Value data;
	Value conflicts;
	Value log;

	PutStatus st;

	st = json2rawdoc(doc, rawdoc, false);
	if (st != PutStatus::stored) return st;

	st = loadDataConflictsLog(doc, &data, &conflicts,&log);
	if (st != PutStatus::stored) return st;

	auto lock = core.lockWrite(h);

	if (core.findDoc(h, rawdoc.docId, rawdoc.revision, prevdoc, tmp)) return PutStatus::stored;

	bool exists = core.findDoc(h,rawdoc.docId, prevdoc, tmp);
	if (exists) {
		Array hl;
		bool found = false;
		//replace of deleted document - log can be empty
		if (rawdoc.deleted && log.empty()) {
			found = true;
		} else  for (Value v:log) {
			RevID pr = parseStrRev(v.getString());
			hl.push_back(pr);
			if (pr == prevdoc.revision) {
				found = true;
				break;
			}
		}
		if (!found) {
			for (Value v:conflicts) {
				RevID pr = parseStrRev(v.getString());
				if (pr == prevdoc.revision) {
					found = true;
					break;
				}
			}
			if (!found) {
				lock.unlock();
				Value resolved;
				if (resolveConflict(h, doc, resolved) && isSuccess(replicator_put(h, resolved, outrev))) {
						replicator_put_history(h, doc);
						return PutStatus::merged;
				}
				return PutStatus::conflict;
			}
		} else {
			Value hst = Value::parseBinary(JsonSource(prevdoc.payload));
			for (Value c:hst) hl.push_back(c.getUInt());
		}
		newhst = Value(hl).slice(0,core.getMaxLogSize(h));
	} else {
		newhst = parseStrRevArr(log);
	}
	serializePayload(newhst, parseStrRevArr(conflicts), data, tmp);
	rawdoc.payload = tmp;
	core.storeUpdate(h,rawdoc);
	tmp.clear();
	outrev = doc["rev"].toString();
	return PutStatus::stored;
}

PutStatus DocumentDB::replicator_put_history(Handle h, const json::Value &doc) {
	DatabaseCore::RawDocument rawdoc;
	DatabaseCore::RawDocument prevdoc;
	std::string tmp;
	Value data;
	Value conflicts;
	Value log;

	PutStatus st;

	st = json2rawdoc(doc, rawdoc, false);
	if (st != PutStatus::stored) return st;

	st = loadDataConflictsLog(doc, &data, &conflicts,&log);
	if (st != PutStatus::stored) return st;

	if (core.findDoc(h, rawdoc.docId, rawdoc.revision,prevdoc, tmp)) return PutStatus::stored;

	serializePayload(parseStrRevArr(log), parseStrRevArr(conflicts), data, tmp);
	rawdoc.payload = tmp;
	core.storeToHistory(h, rawdoc);
	return PutStatus::stored;

}

json::Value DocumentDB::parseLog(std::string_view &payload) {
		Value log = Value::parseBinary(JsonSource(payload), base64);
		return log;
}




Value DocumentDB::parseDocument(const DatabaseCore::RawDocument& doc, OutputFormat format) {


	if (doc.deleted && format != OutputFormat::deleted) return nullptr;

	Object jdoc;
	std::string tmp;

	jdoc.set("id",doc.docId)
			("rev",serializeStrRev(doc.revision))
			("seq",doc.seq_number)
			("deleted",doc.deleted?Value(true):Value())
			("timestamp",doc.timestamp);


	if (static_cast<int>(format & (OutputFormat::data | OutputFormat::log))) {

		std::string_view p = doc.payload;
		Value log = parseLog(p);
		if (format == OutputFormat::data) {
			Value conflicts = serializeStrRevArr(Value::parseBinary(JsonSource(p), base64));
			Value data = Value::parseBinary(JsonSource(p),base64);

			jdoc.set("data", data);
			if (!conflicts.empty())
				jdoc.set("conflicts",conflicts);
		}
		if (format == OutputFormat::log) {
			jdoc.set("log",serializeStrRevArr(log));
		}
	}
	return jdoc;

}

static auto createJsonSerializer(OutputFormat &fmt, DocumentDB::ResultCB &cb) {
	return [fmt, cb](const DatabaseCore::RawDocument &doc) {
		Value v = DocumentDB::parseDocument(doc, fmt);
		if (v.defined() && !cb(v)) return false;
		return true;
	};
}

bool DocumentDB::listDocs(Handle h, const std::string_view& id, bool reversed,
		OutputFormat format, ResultCB&& callback) {
	return core.enumDocs(h,id,reversed,createJsonSerializer(format,callback));
}

bool DocumentDB::listDocs(Handle h, const std::string_view& start,
		const std::string_view& end, OutputFormat format, ResultCB&& callback) {
	return core.enumDocs(h,start,end,createJsonSerializer(format,callback));
}

SeqNum DocumentDB::readChanges(Handle h, const SeqNum &since, bool reversed, OutputFormat format,  ResultCB &&cb) {
	std::string tmp;
	return core.readChanges(h, since, reversed, [&](const DatabaseCore::ChangeRec &rc) {
		DatabaseCore::RawDocument rawdoc;
		if (!core.findDoc(h,rc.docid,rc.revid, rawdoc, tmp)) return true;
		Value v = parseDocument(rawdoc, format);
		if (v.isNull()) return true;
		return cb(v);
	});

}

SeqNum DocumentDB::readChanges(Handle h, const SeqNum &since, bool reversed, OutputFormat format, DocFilter &&flt, ResultCB &&cb) {
	if (flt == nullptr) return readChanges(h,since,reversed,format,std::move(cb));
	std::string tmp;
	return core.readChanges(h, since, reversed,
				[&](const DatabaseCore::ChangeRec &rc) {
		DatabaseCore::RawDocument rawdoc;
		if (!core.findDoc(h,rc.docid, rc.revid, rawdoc, tmp)) return true;
		Value doc = parseDocument(rawdoc, format | OutputFormat::data | OutputFormat::log);
		if (doc.isNull()) return true;
		Value v = flt(doc);
		if (!v.defined()) return true;
		if (format != OutputFormat::data) {
			v = doc.replace("data",Value());
		}
		if (format != OutputFormat::log) {
			v = v.replace("log",Value());
		}
		return cb(v);
	});

}

json::Value DocumentDB::merge3way(Handle, json::Value left_data, json::Value right_data, json::Value base_data, bool &conflicted) {
	conflicted = false;
	if (left_data.type() != json::object || right_data.type() != json::object) {
		conflicted = true;
		return right_data;
	}
	Value ld = recursive_diff(base_data, left_data);
	Value rd = recursive_diff(base_data, right_data);
	Value md = recursive_force_merge(ld,rd,conflicted);
	Value ad = recursive_apply(base_data,md);
	return ad;
}


bool DocumentDB::resolveConflict(Handle h, json::Value newdoc, json::Value &merged) {
	DatabaseCore::DBConfig cfg;
	if (!core.getConfig(h, cfg)) {
		merged = json::undefined;
		return false;
	}


	std::string_view id = newdoc["id"].getString();
	Value olddoc = this->get(h,id,OutputFormat::replication);
	if (olddoc == nullptr) {
		//document not found, so it cannot be merged
		merged = json::undefined;
		return false;
	}


	//determine, which document is new and old
	if (olddoc["timestamp"].getUInt() >newdoc["timestamp"].getUInt() ) std::swap(olddoc, newdoc);

	Value olddoc_rev = olddoc["rev"];
	Value newdoc_rev = newdoc["rev"];
	Value olddoc_log = olddoc["log"];
	Value newdoc_log = newdoc["log"];
	Value olddoc_data = olddoc["data"];
	Value newdoc_data = newdoc["data"];
	Value olddoc_conflicts = olddoc["conflicts"];
	Value newdoc_conflicts = newdoc["conflicts"];

	//Primitive check whether one document is not descendant of other document (or already reported as conflict)
	if (olddoc_log.indexOf(newdoc_rev) != Value::npos || olddoc_conflicts.indexOf(newdoc_rev) != Value::npos
		|| newdoc_log.indexOf(olddoc_rev) != Value::npos || newdoc_conflicts.indexOf(olddoc_rev) != Value::npos) {
		//this is error - we cannot merge it
		merged = json::undefined;
		return false;
	}

	bool olddel = olddoc["deleted"].getBool();
	bool newdel = olddoc["deleted"].getBool();
	//if one of documents is deleted, result is deleted
	//only way to restore deleted document is to update document without conflict
	bool finaldel = olddel || newdel;

	//find base document
	Value basedoc = findBaseDocument(h, id, olddoc_log, newdoc_log);
	Value data;
	bool conflicted;
	//found
	if (basedoc != nullptr) {
		data = merge3way(h, olddoc_data, newdoc_data,basedoc["data"],conflicted);
	} else {
		conflicted = true;
		data = newdoc["data"];
	}

	Array a_conflicts;
	Object resdoc;

	Value log;
	Value conflicts;

	if (conflicted) {

		log = mergeLogs(newdoc_rev, newdoc_log);
		conflicts = mergeLogs(olddoc_rev, newdoc_conflicts, olddoc_conflicts);
	} else {
		log = mergeLogs(newdoc_rev, olddoc_rev, newdoc_log, olddoc_log, basedoc["log"]);
		conflicts = mergeLogs(newdoc_conflicts, olddoc_conflicts);
	}

	conflicts = conflicts.filter([&](Value z){
		return log.indexOf(z) == Value::npos;
	});

	Value timestamp = getTimestamp();
	RevID newrev = calcRevisionID(data,conflicts,timestamp,finaldel);


	resdoc.set("id",StrViewA(id))
		      ("rev", serializeStrRev(newrev))
			  ("log",log.slice(0,cfg.logsize))
			  ("conflicts",conflicts)
			  ("deleted",finaldel)
			  ("timestamp", timestamp)
			  ("data",data);
	merged = resdoc;
	return !conflicted;
}

json::Value DocumentDB::findBaseDocument(Handle h, std::string_view id, json::Value log1, json::Value log2) {
	std::size_t a = 0, pos1 = Value::npos,pos2 = Value::npos;
	for (Value c: log1) {
		std::size_t x = log2.indexOf(c);
		if (x != Value::npos) {
			pos2 = x;
			pos1 = a;
			Value doc = get(h,id,c.getString(), OutputFormat::replication);
			if (doc != nullptr) return doc;
		}
		++a;
	}
	if (pos2 != Value::npos) {
		Value l1 = log1.slice(pos1);
		Value l2 = log2.slice(pos2);
		std::size_t cnt = std::max(l1.size(),l2.size());
		for (std::size_t i = 0; i < cnt; i++) {
			Value a = l1[i];
			Value b = l2[i];
			if (a.defined()) {
				Value doc = get(h,id,a.getString(), OutputFormat::replication);
				if (doc != nullptr) return doc;
			}
			if (b.defined()) {
				Value doc = get(h,id,b.getString(), OutputFormat::replication);
				if (doc != nullptr) return doc;
			}
		}
	}
	return nullptr;
}


} /* namespace sofadb */
