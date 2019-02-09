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
#include <imtjson/string.h>
#include <libsofa/keyformat.h>

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
char *DocumentDB::serializeStrRev(RevID rev, char *out, int leftZeroes) {
	if (rev == 0 && leftZeroes <= 0) return out;
	out = serializeStrRev(rev/62, out, leftZeroes-1);
	RevID d = rev % 62;
	if (d < 10) *out = d+'0';
	else if (d < 36) *out = d-10+'A';
	else *out = d-36+'a';
	return out+1;
}


String DocumentDB::serializeStrRev(RevID rev) {

	constexpr auto digits = 12*sizeof(rev)/8;

	return String(digits, [&](char *c) {
		char *d = serializeStrRev(rev, c,digits);
		return d-c;
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

DocumentDB::Status DocumentDB::createPayload(const json::Value &doc, json::Value &payload) {

	Value data = doc["data"];
	Value conflicts = doc["conflicts"];
	if (!conflicts.defined()) conflicts = json::array;
	else if (conflicts.type() !=json::array) return error_conflicts_must_be_array;

	payload = {data, conflicts};
	return stored;
}

json::Value DocumentDB::get(Handle h, const std::string_view& id, OutputFormat oform) {
	DatabaseCore::RawDocument rdoc;
	std::string tmp;
	if (!core.findDoc(h,id,rdoc,tmp)) return json::Value();
	return parseDocument(rdoc, oform);
}

json::Value DocumentDB::get(Handle h, const std::string_view& id, const std::string_view& rev, OutputFormat oform) {
	DatabaseCore::RawDocument rdoc;
	std::string tmp;
	if (!core.findDoc(h,id,parseStrRev(rev), rdoc,tmp)) return json::Value();
	return parseDocument(rdoc, oform);
}

void DocumentDB::serializePayload(const json::Value &newhst, const json::Value &conflicts, const json::Value &payload,  std::string &tmp) {
	tmp.clear();
	newhst.serializeBinary(JsonTarget(tmp),0);
	conflicts.serializeBinary(JsonTarget(tmp),0);
	payload.serializeBinary(JsonTarget(tmp),json::compressKeys);
}

DocumentDB::Status DocumentDB::json2rawdoc(const json::Value &doc, DatabaseCore::RawDocument  &rawdoc, bool new_edit) {
	Value jid = doc["id"];
	if (jid.type() != json::string) return error_id_must_be_string;
	Value jrev = doc["rev"];
	if (jrev.type() != json::string) {
		if (!new_edit || jrev.defined())
			return error_rev_must_be_string;
	}
	std::string_view id = jid.getString();
	std::uint64_t timestamp;
	if (!new_edit) {
		Value jtimestamp = doc["timestamp"];
		if (jtimestamp.type() != json::number) return error_timestamp_must_be_number;
		timestamp = jtimestamp.getUInt();
	} else {
		timestamp = getTimestamp();
	}
	RevID rev = parseStrRev(jrev.getString());
	Value jdel = doc["deleted"];
	if (jdel.defined() && jdel.type() != json::boolean)
		return error_deleted_must_be_bool;
	rawdoc.deleted = jdel.getBool();
	rawdoc.docId = id;
	rawdoc.revision = rev;
	rawdoc.timestamp = timestamp;
	rawdoc.version = 0;
	return stored;
}


DocumentDB::Status DocumentDB::loadDataConflictsLog(const json::Value &doc,
		Value *data, Value *conflicts, Value *log) {
	if (data) {
		*data = doc["data"];
		if (!data->defined()) return error_data_is_manadatory;
	}
	if (conflicts) {
		*conflicts = doc["conflicts"];
		if (conflicts->defined()) {
			if (conflicts->type() != json::array) return error_conflicts_must_be_array;
			for (Value v: *conflicts)
				if (v.type() != json::string) return error_conflict_must_be_string;
		}
	}
	if (log) {
		*log = doc["log"];
		if (!log->defined()) return error_log_is_mandatory;
		for (Value v: *log)
			if (v.type() != json::string) return error_log_item_must_be_string;
	}
	return stored;
}


DocumentDB::Status DocumentDB::client_put(Handle h, const json::Value &doc, json::String &outrev) {

	DatabaseCore::RawDocument rawdoc;
	DatabaseCore::RawDocument prevdoc;
	std::string tmp;
	Value newhst;
	Status st;
	Value data;
	Value conflicts;

	st = json2rawdoc(doc, rawdoc, true);
	if (st != stored) return st;

	st = loadDataConflictsLog(doc, &data, &conflicts,nullptr);
	if (st != stored) return st;

	RevID newRev;
	Value({data, conflicts, rawdoc.timestamp, rawdoc.deleted}).serializeBinary(FNV1a<sizeof(RevID)>(newRev),0);
	bool prevrev_ndefined = doc["rev"].getString().empty();

	bool exists = core.findDoc(h,rawdoc.docId,prevdoc, tmp);
	if (exists) {
		if (prevrev_ndefined) return conflict;
		RevID revid = rawdoc.revision;
		if (rawdoc.revision != prevdoc.revision) return conflict;
		Value hst = Value::parseBinary(JsonSource(prevdoc.payload));
		Array joinhst;
		joinhst.reserve(hst.size()+1);
		joinhst.push_back(revid);
		joinhst.addSet(hst);
		newhst = Value(joinhst).slice(0,core.getMaxLogSize(h));
	} else {
		if (!prevrev_ndefined) return conflict;
		newhst = json::array;
	}
	serializePayload(newhst, conflicts, data, tmp);
	rawdoc.payload = tmp;
	core.storeUpdate(h,rawdoc);
	tmp.clear();
	outrev = serializeStrRev(newRev);
	return stored;
}



DocumentDB::Status DocumentDB::replicator_put(Handle h, const json::Value &doc) {
	DatabaseCore::RawDocument rawdoc;
	DatabaseCore::RawDocument prevdoc;
	std::string tmp;
	Value newhst;
	Value data;
	Value conflicts;
	Value log;

	Status st;

	st = json2rawdoc(doc, rawdoc, false);
	if (st != stored) return st;

	st = loadDataConflictsLog(doc, &data, &conflicts,&log);
	if (st != stored) return st;

	if (core.findDoc(h, rawdoc.docId, rawdoc.revision, prevdoc, tmp)) return stored;

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
			if (!found) return conflict;
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
	return stored;
}

DocumentDB::Status DocumentDB::replicator_put_history(Handle h, const json::Value &doc) {
	DatabaseCore::RawDocument rawdoc;
	DatabaseCore::RawDocument prevdoc;
	std::string tmp;
	Value data;
	Value conflicts;
	Value log;

	Status st;

	st = json2rawdoc(doc, rawdoc, false);
	if (st != stored) return st;

	st = loadDataConflictsLog(doc, &data, &conflicts,&log);
	if (st != stored) return st;

	if (core.findDoc(h, rawdoc.docId, rawdoc.revision,prevdoc, tmp)) return stored;

	serializePayload(log, conflicts, data, tmp);
	rawdoc.payload = tmp;
	core.storeUpdate(h, rawdoc);
	return stored;

}


Value DocumentDB::parseDocument(const DatabaseCore::RawDocument& doc, OutputFormat format) {


	if (doc.deleted && format != OutputFormat::deleted) return Value();

	Object jdoc;
	std::string tmp;

	jdoc.set("id",doc.docId)
			("rev",serializeStrRev(doc.revision))
			("seq",doc.seq_number)
			("deleted",doc.deleted?Value(true):Value())
			("timestamp",doc.timestamp);


	if (static_cast<int>(format & (OutputFormat::data | OutputFormat::log))) {

		std::string_view p = doc.payload;
		Value log = Value::parseBinary(JsonSource(p), base64);
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


} /* namespace sofadb */
