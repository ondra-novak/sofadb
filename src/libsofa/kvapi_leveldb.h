/*
 * kvapi_leveldb.h
 *
 *  Created on: 5. 2. 2019
 *      Author: ondra
 */

#ifndef SRC_LIBSOFA_KVAPI_LEVELDB_H_
#define SRC_LIBSOFA_KVAPI_LEVELDB_H_

#include "kvapi.h"

namespace sofadb {


PKeyValueDatabase open_leveldb_database(const std::string_view &name);

}



#endif /* SRC_LIBSOFA_KVAPI_LEVELDB_H_ */
