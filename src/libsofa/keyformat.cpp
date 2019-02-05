/*
 * keyformat.cpp
 *
 *  Created on: 5. 2. 2019
 *      Author: ondra
 */

#include <imtjson/value.h>
#include <imtjson/binjson.tcc>
#include "keyformat.h"
#include <sstream>

namespace sofadb {



std::string serializeJSON(const json::Value& v) {

	std::ostringstream buff;
	v.serializeBinary([&](char c) {
		buff.put(c);
	}, 0);
	return buff.str();
}

void serializeJSON(const json::Value& v, std::string &out) {

	v.serializeBinary([&](char c) {
		out.push_back(c);
	}, 0);
}

json::Value parseJSON(const std::string_view& str) {
	std::size_t pos = 0;
	return json::Value::parseBinary([&](){
		return str[pos++];
	},json::base64);
}


}
