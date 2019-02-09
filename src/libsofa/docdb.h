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

enum class OutputFormat {
	///return metadata only
	metadata_only = 0,
	///return user data
	data = 1,
	///return revision log
	log = 2,
	///return data and log
	data_and_log = 3,
	///also return deleted item
	deleted = 4,
	///return user data including deleted
	data_and_deleted = 5,
	///return revision log including deleted
	log_and_deleted = 6,
	///return everytjing
	data_and_log_and_deleted = 7
};

inline OutputFormat operator|(OutputFormat a, OutputFormat b) {
	return static_cast<OutputFormat>(static_cast<int>(a) |  static_cast<int>(b));
}
inline OutputFormat operator&(OutputFormat a, OutputFormat b) {
	return static_cast<OutputFormat>(static_cast<int>(a) &  static_cast<int>(b));
}
inline bool operator == (OutputFormat a, OutputFormat b) {
	int c = (static_cast<int>(a) & static_cast<int>(b));
	return (c == static_cast<int>(b) || c == static_cast<int>(a));
}
inline bool operator != (OutputFormat a, OutputFormat b) {
	return !(a == b);
}

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
		error_conflict_must_be_string,
		error_deleted_must_be_bool,
		error_timestamp_must_be_number,
		error_data_is_manadatory,
		error_log_is_mandatory,
		error_log_item_must_be_string
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

	json::Value get(Handle h, const std::string_view &id, OutputFormat format);

	json::Value get(Handle h, const std::string_view &id, const std::string_view &rev, OutputFormat format);

	typedef std::function<bool(const json::Value &)> ResultCB;

	bool listDocs(Handle h, const std::string_view &id, bool reversed, OutputFormat format, ResultCB &&callback);

	bool listDocs(Handle h, const std::string_view &start, const std::string_view &end, OutputFormat format, ResultCB &&callback);


	static std::uint64_t getTimestamp();

	static json::Value parseDocument(const DatabaseCore::RawDocument &doc, OutputFormat format);

	static RevID parseStrRev(const std::string_view &strrev);
	static char *serializeStrRev(RevID rev, char *out, int leftZeroes = 12);
	static json::String serializeStrRev(RevID rev);

protected:
	DatabaseCore &core;

	static Status createPayload(const json::Value &doc, json::Value &payload);
	static void serializePayload(const json::Value &newhst, const json::Value &conflicts, const json::Value &payload, std::string &tmp);
	static Status json2rawdoc(const json::Value &doc, DatabaseCore::RawDocument  &rawdoc, bool new_edit);
	static Status loadDataConflictsLog(const json::Value &doc, json::Value *data, json::Value *conflicts, json::Value *log);


};

} /* namespace sofadb */

#endif /* SRC_LIBSOFA_DOCDB_H_ */
