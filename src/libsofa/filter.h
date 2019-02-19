#ifndef SRC_LIBSOFA_FILTER_H_29038490238409
#define SRC_LIBSOFA_FILTER_H_29038490238409

#include <functional>
#include <imtjson/value.h>

namespace sofadb {

using DocFilter = std::function<json::Value(const json::Value)>;

DocFilter createFilter(json::Value def);


}


#endif
