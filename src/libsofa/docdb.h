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
#include "filter.h"

namespace sofadb {


struct PutStatus2Error {
	PutStatus status;
	int code;
	std::string_view msg;
};


inline OutputFormat operator|(OutputFormat a, OutputFormat b) {
	return static_cast<OutputFormat>(static_cast<int>(a) |  static_cast<int>(b));
}
inline OutputFormat operator&(OutputFormat a, OutputFormat b) {
	return static_cast<OutputFormat>(static_cast<int>(a) &  static_cast<int>(b));
}
inline OutputFormat operator-(OutputFormat a, OutputFormat b) {
	return static_cast<OutputFormat>(static_cast<int>(a) &  ~static_cast<int>(b));
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
	PutStatus client_put(Handle h, const json::Value &doc, json::String &rev);
	///Puts replicated d1ocument
	/**
	 * The replicated document contains rev which have its actual revision. It must
	 * also contain history to correctly connect the document to the top, It must
	 * have timestamp
	 *
	 * @param doc document to put
	 * @return status
	 */
	PutStatus replicator_put(Handle h, const json::Value &doc);
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
	PutStatus replicator_put_history(Handle h, const json::Value &doc);

	json::Value get(Handle h, const std::string_view &id, OutputFormat format);

	json::Value get(Handle h, const std::string_view &id, const std::string_view &rev, OutputFormat format);

	typedef std::function<bool(const json::Value &)> ResultCB;

	bool listDocs(Handle h, const std::string_view &id, bool reversed, OutputFormat format, ResultCB &&callback);

	bool listDocs(Handle h, const std::string_view &start, const std::string_view &end, OutputFormat format, ResultCB &&callback);

	///read changes
	/**
	 * @param h handle to database
	 * @param since sequence number where to start (last read sequence number or zero)
	 * @param reversed set true if you need reversed list
	 * @param format output format. It always includes deleted items
	 * @param cb callback function. Function must return true to continue or false to stop reading
	 * @return last sequence number. If zero is returned, then invalid handle or database is empty. If reversed is in efect, function returns lowest seqnum processed
	 *
	 */
	SeqNum readChanges(Handle h, const SeqNum &since, bool reversed, OutputFormat format, ResultCB &&cb);

	///read changes with filter
	/**
	 * @param h handle to database
	 * @param since sequence number where to start (last read sequence number or zero)
	 * @param reversed set true if you need reversed list
	 * @param format output format. It always includes deleted items
	 * @param flt user defined filter function. The function accepts Value which contains whole document.
	 * 			It also contains data section even if the data are not specified in output format. However
	 * 			it doesn't contain log section unless the log section is requested explicitly
	 * 			Function can return whole document or undefined to filter document out.
	 * 			The function can also modify the document.
	 * @param cb callback function. . Function must return true to continue or false to stop reading
	 * @return last sequence number. If zero is returned, then invalid handle or database is empty. If reversed is in efect, function returns lowest seqnum processed
	 */
	SeqNum readChanges(Handle h, const SeqNum &since, bool reversed, OutputFormat format, DocFilter &&flt, ResultCB &&cb);

	///Resolves conflict
	/**
	 * @param h handle to database
	 * @param doc document contains update of an document in database.
	 * @return Return doc, if null is not conflicted. Returns merged version document if merge was successful. Returns conflicted version
	 * of the document, if the merge was unsuccessful. Returns null, if document doesn't exists
	 *
	 * In other words, function resolves conflicts only if there are conflicts. The result document should be now submited successfully. However
	 * because race condition can happedm the result document still can be in conflict, so function must be called again
	 *
	 * @note required log and data
	 */
	json::Value resolveConflict(Handle h, json::Value doc);

	///Retrieves common revision from historical database
	/**
	 * @param h handle of database
	 * @param id id of document
	 * @param log1 log of first branch
	 * @param log2 log of second branch
	 * @return Function returns common revision if exists, or nullptr if doesn't exists
	 */
	json::Value getCommonRev(Handle h, std::string_view id, json::Value log1, json::Value log2);

	static RevID calcRevisionID(json::Value data,json::Value conflicts, json::Value timestamp, json::Value deleted);

	static std::uint64_t getTimestamp();

	static json::Value parseDocument(const DatabaseCore::RawDocument &doc, OutputFormat format);

	static RevID parseStrRev(const std::string_view &strrev);
	static std::string_view serializeStrRev(RevID rev, char *out, int leftZeroes = 12);
	static json::String serializeStrRev(RevID rev);

	static json::Value parseLog(std::string_view &payload);

	DatabaseCore &getDBCore() {return core;}
protected:
	DatabaseCore &core;

	static PutStatus createPayload(const json::Value &doc, json::Value &payload);
	static void serializePayload(const json::Value &newhst, const json::Value &conflicts, const json::Value &payload, std::string &tmp);
	static PutStatus json2rawdoc(const json::Value &doc, DatabaseCore::RawDocument  &rawdoc, bool new_edit);
	static PutStatus loadDataConflictsLog(const json::Value &doc, json::Value *data, json::Value *conflicts, json::Value *log);


};

} /* namespace sofadb */

#endif /* SRC_LIBSOFA_DOCDB_H_ */
