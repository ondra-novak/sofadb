/*
 * types.h
 *
 *  Created on: 5. 2. 2019
 *      Author: ondra
 */

#ifndef SRC_LIBSOFA_TYPES_H_
#define SRC_LIBSOFA_TYPES_H_

namespace sofadb {

using DocID = std::string;
using RevID = std::uint64_t;
using SeqNum = std::uint64_t;
using ViewID = std::uint64_t;
using Timestamp = std::uint64_t;

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
	replication = 7
};


enum class PutStatus {
	stored, ///< put successful - document stored unaltered
	merged,  ///< put successful - but document has been merged (top revision is different)
	conflict, ///< conflict - cannot be merged
	db_not_found, ///< database not found
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

inline bool isSuccess(PutStatus st) {return st == PutStatus::stored || st == PutStatus::merged;}


}



#endif /* SRC_LIBSOFA_TYPES_H_ */
