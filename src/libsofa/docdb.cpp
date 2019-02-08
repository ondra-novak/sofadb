/*
 * docdb.cpp
 *
 *  Created on: 7. 2. 2019
 *      Author: ondra
 */

#include <imtjson/array.h>
#include <imtjson/object.h>
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
void DocumentDB::serializeStrRev(RevID rev, std::string &out, int leftZeroes) {
	if (rev == 0 && leftZeroes <= 0) return;
	serializeStrRev(rev/62, out, leftZeroes-1);
	RevID d = rev % 62;
	if (d < 10) out.push_back(d+'0');
	else if (d < 36) out.push_back(d-10+'A');
	else  out.push_back(d-36+'a');
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

template<bool overwrite_timestamp>
DocumentDB::Status DocumentDB::createPayload(const json::Value &doc, json::Value &payload) {

	Value data = doc["data"];
	Value deleted = doc["deleted"];
	Value conflicts = doc["conflicts"];
	Value timestamp;
	if (overwrite_timestamp) {
		timestamp = getTimestamp();
	} else {
		timestamp = doc["timestamp"];
		if (timestamp.type() != json::number) return error_timestamp_must_be_number;
	}

	if (!conflicts.defined()) conflicts = json::array;
	else if (conflicts.type() !=json::array) return error_conflicts_must_be_array;

	if (deleted.defined() && deleted.type() != json::boolean) return error_deleted_must_be_bool;
	else deleted = deleted.getBool();
	payload = {data, timestamp, conflicts,deleted};
	return stored;
}

void DocumentDB::serializePayload(unsigned char version, const json::Value &newhst, const json::Value &payload, std::string &tmp) {
	tmp.clear();
	tmp.push_back(version);
	newhst.serializeBinary(JsonTarget(tmp),0);
	payload.serializeBinary(JsonTarget(tmp),json::compressKeys);
}

DocumentDB::Status DocumentDB::client_put(Handle h, const json::Value &doc, json::String &outrev) {

	Value id = doc["id"];
	if (id.type() != json::string) return error_id_must_be_string;
	Value rev = doc["rev"];
	if (rev.defined() && rev.type() != json::string) return error_rev_must_be_string;


	Value payload;
	Status st = createPayload<true>(doc, payload);
	if (st != stored) return st;

	RevID newRev = 0;
	payload.serializeBinary(FNV1a<sizeof(RevID)>(newRev),0);

	DatabaseCore::RawDocument rawdoc;
	std::string tmp;
	Value newhst;
	bool exists = core.findDoc(h,id.getString(),rawdoc, tmp);
	if (exists) {
		if (rev.getString().empty()) return conflict;
		RevID revid = parseStrRev(rev.getString());
		if (rawdoc.revision != revid) return conflict;
		Value hst = Value::parseBinary(JsonSource(rawdoc.payload));
		Array joinhst;
		joinhst.reserve(hst.size()+1);
		joinhst.push_back(revid);
		joinhst.addSet(hst);
		newhst = joinhst;
	} else {
		if (!rev.getString().empty()) return conflict;
		newhst = json::array;
	}
	serializePayload(0,newhst, payload, tmp);
	rawdoc.docId = id.getString();
	rawdoc.revision = newRev;
	rawdoc.payload = tmp;
	core.storeUpdate(h,rawdoc);
	tmp.clear();
	serializeStrRev(newRev, tmp);
	outrev = String(tmp);
	return stored;
}

DocumentDB::Status DocumentDB::replicator_put(Handle h, const json::Value &doc) {
	Value id = doc["id"];
	if (id.type() != json::string) return error_id_must_be_string;
	Value rev = doc["rev"];
	if (rev.type() != json::string) return error_rev_must_be_string;
	RevID curRev = parseStrRev(rev.getString());

	std::string tmp;
	DatabaseCore::RawDocument rdoc;
	if (core.findDoc(h, id.getString(), curRev,rdoc, tmp)) return stored;

	Value payload;
	Status st = createPayload<false>(doc, payload);
	if (st != stored) return st;


	Value newhst;
	bool exists = core.findDoc(h,id.getString(),rdoc, tmp);
	if (exists) {
		Array h;
		bool found = false;
		for (Value v:doc["log"]) {
			RevID pr = parseStrRev(v.getString());
			if (pr == rdoc.revision) {
				found = true;
				break;
			}
			h.push_back(v);
		}
		if (!found) {
			for (Value v:doc["conflicts"]) {
				RevID pr = parseStrRev(v.getString());
				if (pr == rdoc.revision) {
					found = true;
					break;
				}
			}
			if (!found) return conflict;
		} else {
			Value hst = Value::parseBinary(JsonSource(rdoc.payload));
			h.addSet(hst);
		}
		newhst = h;
	}
	serializePayload(0,newhst, payload, tmp);
	rdoc.revision = curRev;
	rdoc.payload = tmp;
	core.storeUpdate(h, rdoc);
	return stored;
}

DocumentDB::Status DocumentDB::replicator_put_history(Handle h, const json::Value &doc) {
	Value id = doc["id"];
	if (id.type() != json::string) return error_id_must_be_string;
	Value rev = doc["rev"];
	if (rev.type() != json::string) return error_rev_must_be_string;
	RevID curRev = parseStrRev(rev.getString());

	Value payload;
	Status st = createPayload<false>(doc, payload);
	if (st != stored) return st;

	std::string tmp;
	DatabaseCore::RawDocument rdoc;
	if (core.findDoc(h, id.getString(), curRev,rdoc, tmp)) return stored;
	serializePayload(0,doc["log"], payload, tmp);
	rdoc.revision = curRev;
	rdoc.payload = tmp;
	core.storeUpdate(h, rdoc);
	return stored;
}

bool DocumentDB::parsePayload(std::string_view p, unsigned char *version, json::Value *log, json::Value *payload) {
	JsonSource src(p);
	unsigned char v = static_cast<unsigned char>(src());
	if (v != 0) return false;
	Value l = Value::parseBinary(src,base64);
	if (payload) *payload = Value::parseBinary(src,base64);
	if (log) *log = l;
	if (version) *version = v;
	return true;
}



Value DocumentDB::parseDocument(const DatabaseCore::RawDocument& doc, bool withLog) {


	Object jdoc;
	std::string tmp;

	serializeStrRev(doc.revision,tmp);

	jdoc.set("id",doc.docId);
	jdoc.set("rev",tmp);
	jdoc.set("seq",doc.seq_number);


	Value log;
	Value payload;
	parsePayload(doc.payload, nullptr, &log, &payload);

	jdoc.set("data", payload[0]);
	jdoc.set("timestamp", payload[1]);
	if (!payload[2].empty()) jdoc.set("conflicts", payload[2]);
	if (payload[3].getBool()) jdoc.set("deleted",true);

	if (withLog) {
		Array a;
		a.reserve(log.size());
		for (Value v:log) {
			serializeStrRev(v.getUInt(), tmp);
			a.push_back(tmp);
		}
		jdoc.set("log",log);
	}

	return jdoc;

}

} /* namespace sofadb */
