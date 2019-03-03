/*
 * document.h
 *
 *  Created on: 3. 3. 2019
 *      Author: ondra
 */

#ifndef SRC_LIBSOFA_DOCUMENT_H_
#define SRC_LIBSOFA_DOCUMENT_H_
#include <imtjson/object.h>
#include <imtjson/value.h>

namespace sofadb {


class Document: public json::Object {
public:
	Document(json::Value doc)
		:json::Object(doc["data"])
		,id(doc["id"])
		,rev(doc["rev"])
		,deleted(doc["deleted"])
		,conflicts(doc["conflicts"])
		,timestamp(doc["timestamp"])
		,log(doc["log"]) {}

	explicit Document(std::string_view id):id(id) {}
	Document(std::string_view id, std::string_view rev):id(id),rev(rev) {}
	Document(std::string_view id, json::Value data):json::Object(data),id(id) {}
	Document(std::string_view id, std::string_view rev, json::Value data):json::Object(data),id(id),rev(rev) {}

	const json::Value& getConflicts() const {
		return conflicts;
	}

	void setConflicts(const json::Value& conflicts) {
		this->conflicts = conflicts;
	}
	void clearConflicts() {
		this->conflicts = json::Value();
	}

	bool getDeleted() const {
		return deleted.getBool();
	}

	void setDeleted(bool deleted) {
		this->deleted = deleted;
	}

	std::string_view getID() const {
		return id.getString();
	}

	const json::Value& getLog() const {
		return log;
	}

	const std::string_view getRev() const {
		return rev.getString();
	}

	void setRev(const json::Value& rev) {
		this->rev = rev;
	}

	std::size_t getTimestamp() const {
		return timestamp.getUInt();
	}

	operator json::Value() const {
		json::Object res;
		res("id",id)
		   ("rev",rev)
		   ("deleted",deleted)
		   ("conflicts",conflicts)
		   ("data",json::Value(this->commit()));
		return res;
	}

protected:
	json::Value id;
	json::Value rev;
	json::Value deleted;
	json::Value conflicts;
	json::Value timestamp;
	json::Value log;
};


}



#endif /* SRC_LIBSOFA_DOCUMENT_H_ */
