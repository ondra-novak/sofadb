/*
 * api.cpp
 *
 *  Created on: 13. 2. 2019
 *      Author: ondra
 */

#include "api.h"


namespace sofadb {


SofaDB::SofaDB(PKeyValueDatabase kvdatabase)
	:dbcore(kvdatabase)
	,docdb(dbcore)
{

}

Handle SofaDB::createDB(const std::string_view& name) {

}

bool SofaDB::deleteDB(Handle h) {
}

}

