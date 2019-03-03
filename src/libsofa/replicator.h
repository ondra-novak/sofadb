/*
 * replicator.h
 *
 *  Created on: 3. 3. 2019
 *      Author: ondra
 */

#ifndef SRC_LIBSOFA_REPLICATOR_H_
#define SRC_LIBSOFA_REPLICATOR_H_

#include <mutex>
#include <functional>
#include <unordered_map>

#include "eventrouter.h"
#include "docdb.h"
#include "replicationtask.h"

namespace sofadb {
/* Replication document basic structure
 *
 *  id - contains identificator of replication task
 *
 *  data:{
 *   //user controlled fields
 *  	source: "name or url",
 *  	target: "name or url",
 *  	filter: {... filter definition ...},
 *  	since: seq_number, - this field is updated by replicator when replication is finished/stopped
 *  	continuous: true - to continuuos replication
 *  	batch: <size_of_batch> - default 1000 - only applies on external replication
 *  	enabled: true/false - replication runs only if enabled is true
 *  //replicator controlled fields
 *		running: true - if replication is running. If this field is false, the replication finished.
 *		User must clear this field to restart replication
 *		error: { - last error description }
 *
 *  }
 *
 */


///Creates replication server from JSON definition - for external replication
using ExternalReplicationFactory = std::function<IReplicationProtocol *(json::Value)>;

class Document;

class Replicator {
public:
	using Handle = DatabaseCore::Handle;

	Replicator(DocumentDB &db, const PEventRouter &rt, ExternalReplicationFactory &&erf);
	~Replicator();


protected:
	class MyTask;
	using Sync = std::unique_lock<std::recursive_mutex> ;
	using PReplication = std::unique_ptr<MyTask>;
	using ReplMap = std::unordered_map<std::string, PReplication>;


	DocumentDB &db;
	PEventRouter rt;
	ExternalReplicationFactory erf;
	Handle h;
	EventRouter::WaitHandle wh;
	std::recursive_mutex lock;
	std::condition_variable_any *exitWait = nullptr;
	SeqNum since = 0;
	bool bootstrap = true;

	ReplMap replMap;

	void dispatchRequests();
	void worker();
	void workerExit();
	void update(Document doc);

	PReplication createTask(const std::string &name, Document &doc);
	IReplicationProtocol *createProtocol(json::Value def);

	void onFinish(const std::string &name,SeqNum seqnum);
	void onUpdate(const std::string &name,SeqNum seqnum);
	void onWarning(const std::string &name,ReplicationTask::Side side, int code, std::string &&msg);


};

} /* namespace sofadb */

#endif /* SRC_LIBSOFA_REPLICATOR_H_ */
