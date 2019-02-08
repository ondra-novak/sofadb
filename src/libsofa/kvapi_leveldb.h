/*
 * kvapi_leveldb.h
 *
 *  Created on: 5. 2. 2019
 *      Author: ondra
 */

#ifndef SRC_LIBSOFA_KVAPI_LEVELDB_H_
#define SRC_LIBSOFA_KVAPI_LEVELDB_H_

#include "kvapi.h"
#include <leveldb/db.h>

namespace sofadb {

class LevelDBException: public std::exception {
public:

	LevelDBException(const leveldb::Status &st);

	const leveldb::Status &getStatus() const {return status;}

protected:
	leveldb::Status status;
	mutable std::string msg;

	const char *what() const noexcept;

};


PKeyValueDatabase leveldb_open(const leveldb::Options& options, const std::string& name);



}



#endif /* SRC_LIBSOFA_KVAPI_LEVELDB_H_ */
