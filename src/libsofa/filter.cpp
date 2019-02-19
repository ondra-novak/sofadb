#include <imtjson/namedEnum.h>
#include <imtjson/string.h>
#include  <imtjson/operations.h>
#include "filter.h"


namespace sofadb {
using namespace json;

/*
 *   filter defined by JSON.
 *   I wanted to use mango query, buy... it is awful. So let define different syntax
 *
 *
 *   Expression
 *   - a constant can be number, string, bool or null
 *   - array is interpreted as path to value in document
 *      ["aaa","bbb",1,2] -> aaa.bbb[1][2].
 *   - object is used for calculation
 *
 *
 *   Basic structure - object is used as test expression
 *   {
 *   	"source": defines source field
 *   	">": target
 *   	"<": target
 *   	"=": target
 *   	"=>": target
 *   	"=<": target
 *   	"!=": target
 *   }
 *
 *   all above operators are tested agains source. Target can be either value or other field
 *
 *   Example
 *   {
 *   	"source":["data","age"]
 *   	">=":18,
 *   	"<=":50
 *   }
 *   Above example states, that data.age must be in specified range. What if age
 *   is not defined? In this case, expression is false. If you need to
 *   return true when age is not defined, you need to use keyword "undefined"
 *
 *   Example
 *   {
 *   	"source":["data","age"]
 *   	"undefined":true<-- states, that doesn't need to be defined
 *   	">=":18,
 *   	"<=":50,
 *   }
 *
 *   Bool logic
 *   {DocFilter createFilter(json::Value filter);
 *   	"and": [ .... tests ... ]
 *   }
 *   {
 *   	"or": [ .... tests ... ]
 *   }
 *
 *
 *   Combination:
 *   {
 *   	"and" [{
 *   		"or":[...,...]
 *   		},{
 *   		"or":[...,...]
 *   		]
 *   }
 *
 *   Note if there is "and" keyword or "or" keyword, other keywords are ignored.
 *   Both "and" or "or" must be alone
 *
 *   Basic calculator
 *   {
 *      "+": [expr, expr]
 *      "-": [expr, expr]
 *      "/": [expr, expr]
 *      "*": [expr, expr]
 *      "iff":[test, expr, expr]
 *      "toNumber":expr
   	   	"toString":expr
 *   }
 *
 *
 *
 *
 *
 *
 */

enum class Operation {
	op_and,
	op_or,
	op_plus,
	op_minus,
	op_mult,
	op_div,
	op_iff,
	op_less,
	op_great,
	op_le,
	op_ge,
	op_eq,
	op_neq,
	op_source,
	op_undefined,
	op_tonumber,
	op_tostring,
	op_prefix,
	op_suffix,
};

NamedEnum<Operation> operationName({
	{Operation::op_and,"and"},
	{Operation::op_or,"or"},
	{Operation::op_plus,"+"},
	{Operation::op_minus,"-"},
	{Operation::op_mult,"*"},
	{Operation::op_div,"/"},
	{Operation::op_iff,"iff"},
	{Operation::op_less,"<"},
	{Operation::op_great,">"},
	{Operation::op_le,"<="},
	{Operation::op_ge,">="},
	{Operation::op_eq,"="},
	{Operation::op_neq,"!="},
	{Operation::op_prefix,"prefix"},
	{Operation::op_suffix,"suffix"},
	{Operation::op_tonumber,"toNumber"},
	{Operation::op_tostring,"toString"},
	{Operation::op_source,"source"},
	{Operation::op_undefined,"undefined"},
});

static Value expression(const Value &def, const Value &doc);
static bool runFilter(const Value &def, const Value &doc) {
	Value a = def[0];
	StrViewA key = a.getKey();
	switch(operationName[key]) {
	case Operation::op_and:
		for(Value x:  a) {
			if (!runFilter(x, doc)) return false;
		}
		return true;
	case Operation::op_or:
		for(Value x:  a) {
			if (runFilter(x, doc)) return true;
		}
		return false;
	default: {
		Value source = def["source"];
		if (source.defined()) {

			Value src = expression(source, doc);
			if (src.defined()) {

				for (Value x: def) {
					StrViewA key = x.getKey();
					Value trg = expression(x,doc);
					switch (operationName[key]) {
					case Operation::op_eq:
						if (!(Value::compare(src,trg) == 0)) return false;
						break;
					case Operation::op_neq:
						if (!(Value::compare(src,trg) != 0)) return false;
						break;
					case Operation::op_less:
						if (!(Value::compare(src,trg) < 0)) return false;
						break;
					case Operation::op_great:
						if (!(Value::compare(src,trg) > 0)) return false;
						break;
					case Operation::op_le:
						if (!(Value::compare(src,trg) <= 0)) return false;
						break;
					case Operation::op_ge:
						if (!(Value::compare(src,trg) >= 0)) return false;
						break;
					case Operation::op_prefix:
						if (!(StrViewA(src.toString()).begins(trg.toString()))) return false;
						break;
					case Operation::op_suffix:
						if (!(StrViewA(src.toString()).ends(trg.toString()))) return false;
						break;
					default:
						break;
					}
				}
				return true;

			} else {
				return def["undefined"].getBool();
			}
		  }
		}
	}
	return false;
}

static Value expression(const Value &def, const Value &doc) {
	switch (def.type()) {
	case json::object: {
		Value a = def[0];
		StrViewA key = a.getKey();
		switch(operationName[key]) {
		case Operation::op_plus: return a.reduce([&](const Value &a, const Value &b) {
				if (a.defined()) return a.merge(expression(b,doc)); else return expression(a,doc);
			},Value());
		case Operation::op_minus: return a.reduce([&](const Value &a, const Value &b) {
				if (a.defined()) return a.diff(expression(b,doc)); else return expression(a,doc);
			},Value());
		case Operation::op_mult: return a.reduce([&](const Value &a, const Value &b) -> Value {
				if (a.defined()) return a.getNumber()*expression(b,doc).getNumber(); else return expression(a,doc);
			},Value());
		case Operation::op_div: return a.reduce([&](const Value &a, const Value &b) -> Value {
				if (a.defined()) return a.getNumber()/expression(b,doc).getNumber(); else return expression(a,doc);
			},Value());
		case Operation::op_iff: return runFilter(a[0],doc)?expression(a[1],doc):expression(a[2],doc);
		case Operation::op_tonumber: return Value(expression(a,doc).getNumber());
		case Operation::op_tostring: return Value(expression(a,doc).toString());
		default: return runFilter(def, doc);
		}
	}
	break;
	case json::array: {

		Value p = doc;
		for (Value x: def) {
			if (x.type() == json::string) p = p[x.getString()];
			else if (x.type() == json::number) p = p[x.getUInt()];
		}
		return p;
	}break;
	default:
		return def;
	}
return false;
}

DocFilter createFilter(Value def) {
	if (def.defined())
		return DocFilter([=](const Value &doc)->Value{
			return runFilter(def,doc)?doc:Value();
		});
	else {
		return nullptr;
	}






}


}
