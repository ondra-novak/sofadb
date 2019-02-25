/*
 * keyformat.h
 *
 *  Created on: 5. 2. 2019
 *      Author: ondra
 */

#ifndef SRC_LIBSOFA_KEYFORMAT_H_
#define SRC_LIBSOFA_KEYFORMAT_H_

#include <string_view>
#include <string>

namespace json {
	class Value;
}

namespace sofadb {


enum class IndexType {

	db_map = 1,   			///<list of DBS, map DB -> ID
	dbconfig = 2,			///<various keys related to DBS
	seq = 3,				///<sequence numbers
	docs = 4,				///<documents
	doc_revs = 5,			///<document-revision map (historical)
	view_map = 6,			///<view map = key->value
	view_docs = 7,			///doc->keys
	view_state = 8,			///db,viewid -> seqnum
	reduce_map = 9,			///key->value for reduce
	object_index = 10,

};

namespace _misc {
	inline void serialize_key(std::string &, bool ) {}
	template<typename ... Args>
	inline void serialize_key(std::string &key, bool needSep, std::uint64_t x, Args && ... args);
	template<typename ... Args>
	inline void serialize_key(std::string &key, bool needSep, std::uint32_t x, Args && ... args);
	template<typename ... Args>
	inline void serialize_key(std::string &key, bool needSep, unsigned char x, Args && ... args);

	inline void addSep(std::string &key) {
		key.push_back(0);
		key.push_back(0);
	}

	template<typename ... Args>
	inline void serialize_key(std::string &key, bool needSep, const std::string_view &x, Args && ... args) {
		if (needSep) addSep(key);
		key.append(x);
		serialize_key(key, true, std::forward<Args>(args)...);
	}


	template<typename ... Args>
	inline void serialize_key(std::string &key, bool needSep, std::uint64_t x, Args && ... args) {
		if (needSep) addSep(key);
		for (int i = 0; i < 8; i++)
			key.push_back(static_cast<char>((x >> (8*(8-i-1))) & 0xFF));
		serialize_key(key, false, std::forward<Args>(args)...);
	}

	template<typename ... Args>
	inline void serialize_key(std::string &key, bool needSep, unsigned char x, Args && ... args) {
		if (needSep) addSep(key);
		key.push_back(static_cast<char>(x));
		serialize_key(key, false, std::forward<Args>(args)...);
	}

	template<typename ... Args>
	inline void serialize_key(std::string &key, bool needSep, std::uint32_t x, Args && ... args) {
		if (needSep) addSep(key);
		for (int i = 0; i < 4; i++)
			key.push_back(static_cast<char>((x >> (8*(4-i-1))) & 0xFF));
		serialize_key(key, false, std::forward<Args>(args)...);
	}

}

class KCursor {
public:
	std::size_t pos = 0;
	template<typename X>
	auto operator()(X &&x) const {
		return x.substr(pos);
	}
	std::size_t operator=(std::size_t pos) {
		this->pos = pos;
		return pos;
	}
};

template<typename ... Args>
inline void build_key(std::string &key, IndexType t, Args&& ... args) {
	key.clear();
	key.push_back(static_cast<char>(t));
	_misc::serialize_key(key, false, std::forward<Args>(args)...);
}

inline void key_db_map(std::string &key) {build_key(key, IndexType::db_map);}
inline void key_db_map(std::string &key, std::uint32_t dbid) {
	build_key(key, IndexType::db_map, dbid);
}

inline void key_dbconfig(std::string &key, std::uint32_t dbid) {
	build_key(key, IndexType::dbconfig, dbid);
}
inline void key_dbconfig(std::string &key, std::uint32_t dbid, const std::string_view &field) {
	build_key(key, IndexType::dbconfig, dbid, field);
}
inline void key_seq(std::string &key, std::uint32_t dbid) {
	build_key(key, IndexType::seq, dbid);
}
inline void key_seq(std::string &key, std::uint32_t dbid, std::uint64_t seqid) {
	build_key(key, IndexType::seq, dbid, seqid);
}
inline void key_docs(std::string &key, std::uint32_t dbid) {
	build_key(key, IndexType::docs, dbid);
}
inline void key_docs(std::string &key, std::uint32_t dbid, const std::string_view &docid) {
	build_key(key, IndexType::docs, dbid, docid);
}
inline void key_doc_revs(std::string &key, std::uint32_t dbid) {
	build_key(key, IndexType::doc_revs, dbid);
}
inline void key_doc_revs(std::string &key, std::uint32_t dbid, const std::string_view &docid) {
	build_key(key, IndexType::doc_revs, dbid,docid);
}
inline void key_doc_revs(std::string &key, std::uint32_t dbid, const std::string_view &docid, const std::uint64_t &revid) {
	build_key(key, IndexType::doc_revs, dbid, docid,revid);
}
inline void key_view_map(std::string &key, std::uint32_t dbid, std::uint32_t viewid) {
	build_key(key, IndexType::view_map, dbid, viewid);
}
inline void key_view_map(std::string &key, std::uint32_t dbid, std::uint32_t viewid, const std::string_view &keys) {
	build_key(key, IndexType::view_map, dbid, viewid, keys);
}
inline void key_view_map(std::string &key, std::uint32_t dbid, std::uint32_t viewid, const std::string_view &keys, const std::string_view &docid) {
	build_key(key, IndexType::view_map,dbid, viewid, keys, docid);
}
inline void key_view_docs(std::string &key, std::uint32_t dbid, std::uint32_t viewid) {
	build_key(key, IndexType::view_docs,dbid, viewid);
}
inline void key_view_docs(std::string &key, std::uint32_t dbid, std::uint32_t viewid, const std::string_view &docid) {
	build_key(key, IndexType::view_docs, viewid, docid);
}
inline void key_view_state(std::string &key) {
	build_key(key, IndexType::view_docs);
}
inline void key_view_state(std::string &key, std::uint32_t dbid) {
	build_key(key, IndexType::view_docs,dbid);
}
inline void key_view_state(std::string &key, std::uint32_t dbid, std::uint32_t viewid) {
	build_key(key, IndexType::view_docs,dbid, viewid);
}
inline void key_reduce_map(std::string &key, std::uint32_t dbid, std::uint32_t reduceid) {
	build_key(key, IndexType::reduce_map,dbid, reduceid);
}
inline void key_reduce_map(std::string &key, std::uint32_t dbid, std::uint32_t reduceid, const std::uint64_t &keys) {
	build_key(key, IndexType::reduce_map,dbid, reduceid, keys);
}
inline void key_object_index(std::string &key, std::uint32_t dbid,  std::uint64_t idx) {
	build_key(key, IndexType::object_index,dbid, idx);
}
inline void key_object_index(std::string &key, std::uint32_t dbid) {
	build_key(key, IndexType::object_index,dbid);
}

inline unsigned int extract_from_key(const std::string_view &, std::size_t );

inline unsigned int extract_from_key(const std::string_view &, std::size_t skip, KCursor &cursor) {
	cursor = skip;
	return 1;
}

template<typename ... Args> inline unsigned int extract_from_key(const std::string_view &key, std::size_t skip, std::uint32_t &v, Args &... vars);
template<typename ... Args> inline unsigned int extract_from_key(const std::string_view &key, std::size_t skip, std::string_view &v, Args &... vars);

template<typename ... Args>
inline unsigned int extract_from_key(const std::string_view &key, std::size_t skip, std::uint64_t &v, Args &... vars) {
	if (key.length()<skip+8) return 0;
	v = 0;
	for (std::size_t i = 0; i < 8; i++) {
		v = (v << 8) | static_cast<unsigned char>(key[i+skip]);
	}
	return 1+extract_from_key(key,skip+8,vars...);
}



template<typename ... Args>
inline unsigned int extract_from_key(const std::string_view &key, std::size_t skip, std::uint32_t &v, Args &... vars) {
	if (key.length()<skip+4) return 0;
	v = 0;
	for (std::size_t i = 0; i < 4; i++) {
		v = (v << 8) | static_cast<unsigned char>(key[i+skip]);
	}
	return 1+extract_from_key(key,skip+4,vars...);
}

template<typename ... Args>
inline unsigned int extract_from_key(const std::string_view &key, std::size_t skip, std::string &v, Args &... vars) {
	std::size_t pos = skip;
	std::size_t l = key.length();
	v.clear();
	while (pos < l) {
		char c = key[pos];
		if (c == 0 && pos+1<l && key[pos+1] == 0) {
			pos+=2;break;
		}
		v.push_back(c);
		++pos;
	}
	return 1+extract_from_key(key,pos,vars...);
}

template<typename ... Args>
inline unsigned int extract_from_key(const std::string_view &key, std::size_t skip, unsigned char &v, Args &... vars) {
	if (key.length()<skip+1) return 0;
	v = static_cast<unsigned char>(key[skip]);
	return 1+extract_from_key(key,skip+1,vars...);
}

template<typename ... Args>
inline unsigned int extract_from_key(const std::string_view &key, std::size_t skip, char &v, Args &... vars) {
	if (key.length()<skip+1) return 0;
	v = key[skip];
	return 1+extract_from_key(key,skip+1,vars...);
}

template<typename ... Args>
inline unsigned int extract_from_key(const std::string_view &key, std::size_t skip, std::string_view &v, Args &... vars) {
	std::size_t pos = skip;
	std::size_t l = key.length();
	while (pos < l) {
		char c = key[pos];
		if (c == 0 && pos+1<l && key[pos+1] == 0) {
			pos+=2;break;
		}
		++pos;
	}
	v = std::string_view(key.data()+skip, pos-skip);
	return 1+extract_from_key(key,pos,vars...);
}

inline unsigned int extract_from_key(const std::string_view &, std::size_t ) {
	return 0;
}

template<typename ... Args>
inline void serialize_value(std::string &value, Args && ... data) {
	value.clear();
	_misc::serialize_key(value,false,std::forward<Args>(data)...);
}
template<typename ... Args>
inline void extract_value(const std::string_view &value, Args & ... data) {
	extract_from_key(value,0,data...);
}


class JsonSource {
public:
	JsonSource(std::string_view &source):source(source) {}
	int operator()() {
		if (source.empty()) return 2;//Binjson defines 2 as undefined
		else {
			char c = source[0];
			source = source.substr(1);
			return c;
		}
	}
protected:
	std::string_view &source;
};

class JsonTarget {
public:
	JsonTarget(std::string &target):target(target) {}
	void operator()(char c) {
		target.push_back(c);
	}
protected:
	std::string &target;
};

}

#endif /* SRC_LIBSOFA_KEYFORMAT_H_ */
;
