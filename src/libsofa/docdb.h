/*
 * docdb.h
 *
 *  Created on: 7. 2. 2019
 *      Author: ondra
 */

#ifndef SRC_LIBSOFA_DOCDB_H_
#define SRC_LIBSOFA_DOCDB_H_


#include <imtjson/value.h>
#include "types.h"
#include "databasecore.h"

namespace sofadb {

class DocumentDB {
public:
	DocumentDB(DatabaseCore &core);


	enum Status {
		stored,
		conflict,
		db_not_found,
		error_id_must_be_string,
		error_rev_must_be_string,
		error_conflicts_must_be_array,
		error_deleted_must_be_bool,
		error_timestamp_must_be_number
	};

	typedef DatabaseCore::Handle Handle;

	///Put new edit document
	/** The document contains reference to previous revision in rev field,
	 * It doesn't contain history. It may contain conflicts. It cannot contain
	 * timestamp (it is overwritten)
	 *
	 * @param doc document to put
	 * @param rev stores document's revision when it is succesfully stored
	 * @return status status
	 *
	 * @caller can open batch at DatabaseCore if it need to put documents atomically
	 */
	Status client_put(Handle h, const json::Value &doc, json::String &rev);
	///Puts replicated d1ocument
	/**
	 * The replicated document contains rev which have its actual revision. It must
	 * also contain history to correctly connect the document to the top, It must
	 * have timestamp
	 *
	 * @param doc document to put
	 * @return status
	 */
	Status replicator_put(Handle h, const json::Value &doc);
	///Puts document to the history.
	/** The document is stored as history, doesn't change current top
	 *
	 * @param doc historical doc. It must have timestamp and rev. It doesn't need
	 * to have history, because the history is not connected. However it is also
	 * conflicted revision, the history is need to find common revision to make
	 * 3-way merge
	 *
	 * @return status
	 */
	Status replicator_put_history(Handle h, const json::Value &doc);


	json::Value get(Handle h, const std::string_view &id, bool withlog);

	json::Value get(Handle h, const std::string_view &id, const std::string_view &rev, bool withlog);



	static std::uint64_t getTimestamp();

	static json::Value parseDocument(const DatabaseCore::RawDocument &doc, bool withLog);

	static RevID parseStrRev(const std::string_view &strrev);
	static void serializeStrRev(RevID rev, std::string &out, int leftZeroes=11);

protected:
	DatabaseCore &core;

	template<bool overwrite_timestamp>
	static Status createPayload(const json::Value &doc, json::Value &payload);
	static void serializePayload(unsigned char version, const json::Value &newhst, const json::Value &payload, std::string &tmp);
	static bool parsePayload(std::string_view p, unsigned char *version, json::Value *log, json::Value *payload);
};

} /* namespace sofadb */

#endif /* SRC_LIBSOFA_DOCDB_H_ */
