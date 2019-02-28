/*
 * merge.cpp
 *
 *  Created on: 28. 2. 2019
 *      Author: ondra
 */


#include <imtjson/object.h>
#include "merge.h"
namespace sofadb {

using namespace json;

template<typename Coll, typename Left, typename Right, typename Both>
void merge_template(json::Value a, json::Value b, Left &&left, Right &&right, Both &&both, Coll &out) {
	auto ia = a.begin();
	auto ib = b.begin();
	auto ea = a.end();
	auto eb = b.end();
	while (ia != ea && ib != eb) {
		Value va = *ia;
		Value vb = *ib;
		StrViewA kf = va.getKey();
		StrViewA kt = vb.getKey();
		if (kf < kt) {
			left(va,out);
			++ia;
		} else if (kf > kt) {
			right(vb,out);
			++ib;
		} else {
			both(va,vb,out);
			++ia;
			++ib;
		}
	}
	while (ia != ea) {
		left(*ia,out);
		++ia;
	}
	while (ib != eb) {
		right(*ib,out);
		++ib;
	}
}

void setFn(const Value &v, Object &out) {out.set(v);}
void eraseFn(const Value &v, Object &out) {out.set(v.getKey(),json::undefined);}

json::Value recursive_diff(json::Value from, json::Value to) {

	Object out;
	merge_template(from, to, eraseFn,setFn,[](const Value &f, const Value &t, Object &out) {
		if (f.type() == json::object && t.type() == json::object) {
			Value res = recursive_diff(f,t);
			if (res.type() != json::object || !res.empty()) out(f.getKey(),res);
		} else if (f != t) {
			out.set(t);
		}
	},out);
	return out.commitAsDiff();
}

json::Value recursive_merge(json::Value a, json::Value b) {
	Object out;
	bool conflict = false;
	merge_template(a, b, setFn, setFn ,[&](const Value &a, const Value &b, Object &out) {
		if (a.type() == json::object && b.type() == json::object) {
			Value res = recursive_merge(a,b);
			if (res.defined()) out(a.getKey(),res);
			conflict = true;
		} else if (a != b) {
			conflict = true;
		}
	},out);
	if (conflict) return Value();
	return out.commitAsDiff();
}

json::Value recursive_apply(json::Value base, json::Value diff) {
	Object out;
	merge_template(base, diff, setFn, setFn ,[&](const Value &base, const Value &diff, Object &out) {
		if (base.type() == json::object && diff.type() == json::object) {
			Value res = recursive_apply(base,diff);
			out.set(base.getKey(),res);
		} else {
			out.set(diff);
		}
	},out);
	return out;
}



}

