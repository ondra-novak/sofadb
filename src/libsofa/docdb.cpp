/*
 * docdb.cpp
 *
 *  Created on: 7. 2. 2019
 *      Author: ondra
 */

#include <imtjson/array.h>
#include <imtjson/object.h>
#include <imtjson/binjson.tcc>
#include <libsofa/docdb.h>
#include <imtjson/fnv.h>
#include <imtjson/operations.h>
#include <imtjson/string.h>
#include <libsofa/keyformat.h>
#include <libsofa/merge.h>

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

PutStatus DocumentDB::client_put(Handle h, const json::Value &doc, json::String &outrev) {

	DatabaseCore::RawDocument rawdoc;
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
}



PutStatus DocumentDB::replicator_put(Handle h, const json::Value &doc) {
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

	if (core.findDoc(h, rawdoc.docId, rawdoc.revision, prevdoc, tmp)) return PutStatus::stored;

	bool exists = core.findDoc(h,rawdoc.docId, prevdoc, tmp);
	if (exists) {
		Array hl;
		bool found = false;
		for (Value v:log) {
			RevID pr = parseStrRev(v.getString());
			if (pr == prevdoc.revision) {
				found = true;
				break;
			}
			hl.push_back(v);
		}
		if (!found) {
			for (Value v:conflicts) {
				RevID pr = parseStrRev(v.getString());
				if (pr == prevdoc.revision) {
					found = true;
					break;
				}
			}
			if (!found) return PutStatus::conflict;
		} else {
			Value hst = Value::parseBinary(JsonSource(prevdoc.payload));
			hl.addSet(hst);
		}
		newhst = Value(hl).slice(0,core.getMaxLogSize(h));
	}
	serializePayload(newhst, conflicts, data, tmp);
	rawdoc.payload = tmp;
	core.storeUpdate(h,rawdoc);
	tmp.clear();
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

	serializePayload(log, conflicts, data, tmp);
	rawdoc.payload = tmp;
	core.storeUpdate(h, rawdoc);
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
			Value conflicts = Value::parseBinary(JsonSource(p), base64);
			Value data = Value::parseBinary(JsonSource(p),base64);

			Array c;
			for (Value v:conflicts) {
				c.push_back(serializeStrRev(v.getUInt()));
			}

			jdoc.set("data", data);
			if (!c.empty())
				jdoc.set("conflicts",c);
		}
		if (format == OutputFormat::log) {
			Array c;
			for (Value v:log) {
				c.push_back(serializeStrRev(v.getUInt()));
			}
			jdoc.set("log",c);
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

static Value mergeConflictLog(Value c1, Value c2) {
	Array a;
	a.addSet(c1);
	a.addSet(c2);
	Value v = a;
	if (v.empty()) return json::undefined; else return v.sort(Value.compare).uniq();
}

static Value mergeConflictLog(Value c1, Value c2, Value revid) {
	Array a;
	a.addSet(c1);
	a.addSet(c2);
	a.add(revid);
	Value v = a;
	if (v.empty()) return json::undefined; else return v.sort(Value.compare).uniq();
}

static Value createConflictedRev(Value doc1, Value doc2) {
	std::uint64_t tm1 = doc1["timestamp"].getUInt();
	std::uint64_t tm2 = doc2["timestamp"].getUInt();
	if (tm1 < tm2) std::swap(doc1,doc2);
	Object res(doc1);
	Value tm = DocumentDB::getTimestamp();
	Value conflicts = mergeConflictLog(doc1["conflicts"],doc2["conflicts"],doc2["rev"]);
	Value rev = DocumentDB::serializeStrRev(
			DocumentDB::calcRevisionID(doc1["data"],conflicts,tm,doc1["deleted"])
	);
	Value log = Array(doc1["log"]).add(doc1["rev"]);
	res("rev",rev)
	   ("timestamp",timestamp)
	   ("log",log)
	   ("conflicts",conflicts);
	return res;
}

static Value createMergedRev(Value doc1, Value doc2, Value mergedData,bool deleted) {
	Object res(doc1);
	Value tm = DocumentDB::getTimestamp();
	Value conflicts = mergeConflictLog(doc1["conflicts"],doc2["conflicts"]);
	Value rev = DocumentDB::serializeStrRev(
			DocumentDB::calcRevisionID(mergedData,conflicts,tm,deleted)
	);
	Value log = Array(doc1["log"]).add(doc1["rev"]).add(doc2["rev"]);
	res("rev",rev)
	   ("timestamp",timestamp)
	   ("log",log)
	   ("conflicts",conflicts)
	   ("data",mergedData)
	   ("deleted",deleted?Value(true):Value());
	return res;


}


json::Value DocumentDB::resolveConflict(Handle h, json::Value doc) {
	std::string_view id = doc["id"].getString();
	Value log = doc["log"];
	Value data = doc["data"];
	if (!log.defined() || !data.defined()) return nullptr;

	Value mydoc = this->get(h,id,OutputFormat::data_and_log_and_deleted);
	if (mydoc == nullptr) return mydoc;
	Value myrev = mydoc["rev"];
	if (log.indexOf(myrev) != Value::npos) return nullptr;
	Value mydata = mydoc["data"];
	Value mylog = mydoc["log"];
	Value timestamp = getTimestamp();

	//Simple merge per key
	if (mydata.type() == json::object && data.type() == json::object) {
		Value common = getCommonRev(h,id,log,mylog);
		if (common != nullptr) {
			Value cdata = common["data"];
			Value diff1 = recursive_diff(cdata,mydata);
			Value diff2 = recursive_diff(cdata,data);
			Value merge = recursive_merge(diff1,diff2);
			if (merge.defined()) {
				Value m2 = recursive_apply(cdata, merge);
				Array newlog;
				newlog.add(doc["rev"]);
				newlog.add(myrev);
				newlog.addSet(log);
				Object result(doc);
				Value conflicts = mergeFlagArray(doc["conflicts"],mydoc["conflicts"]);
				result.set("rev",serializeStrRev(calcRevisionID(m2,conflicts,timestamp,doc["deleted"])));
				result.set("log",newlog);
				result.set()
				return result;
			}
		}
	}
	{
		Value c1 = doc["conflicts"];
		Value c2 = mydoc["conflicts"];
		Value cc = (Array(c1).addSet(c2));
		cc = cc.sort(Value::compare).uniq();
		if (doc["timestamp"].getUInt()<mydoc["timestamp"].getUInt()) {
			std::swap(doc, mydoc);
		}
		Object result(doc);
		result.set("confli")

	}

}

} /* namespace sofadb */
