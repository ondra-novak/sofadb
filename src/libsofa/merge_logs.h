#include <imtjson/array.h>
#include <imtjson/value.h>
#include <unordered_set>


namespace sofadb {


void mergeLogs2(json::Array &) {
	//empty
}



template<typename ... Args>
void mergeLogs2(json::Array &coll, const json::Value &v, Args&& ... args) {
	mergeLogs2(coll, std::forward<Args>(args)...);
	if (v.type() == json::array) coll.addSet(v.reverse());
	else coll.add(v.stripKey());
}


template<typename ... Args>
json::Value mergeLogs(json::Value logitem, Args&& ... args) {

	json::Array coll;
	mergeLogs2(coll, logitem, std::forward<Args>(args)...);

	std::unordered_set<std::string_view> c;

	return json::Value(coll).filter(
			[&](json::Value v) {
		std::string_view str = v.getString();
		if (c.find(str) == c.end()) {
			c.insert(str);
			return true;
		} else {
			return false;
		}
	}).reverse();

}


}
