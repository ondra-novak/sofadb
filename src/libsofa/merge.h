/*
 * merge.h
 *
 *  Created on: 28. 2. 2019
 *      Author: ondra
 */

#ifndef SRC_LIBSOFA_MERGE_H_
#define SRC_LIBSOFA_MERGE_H_

#include <imtjson/value.h>
namespace sofadb {


///diff = changes from object "from" to object "to"
json::Value recursive_diff(json::Value from, json::Value to);
///merges two diffs, return undefined if there is conflict
json::Value recursive_merge(json::Value a, json::Value b);
///applies diff to base object
json::Value recursive_apply(json::Value base, json::Value diff);

}



#endif /* SRC_LIBSOFA_MERGE_H_ */
