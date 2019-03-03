/*
 * replicationtask.h
 *
 *  Created on: 28. 2. 2019
 *      Author: ondra
 */

#ifndef SRC_LIBSOFA_REPLICATIONTASK_H_
#define SRC_LIBSOFA_REPLICATIONTASK_H_
#include <shared/worker.h>
#include <memory>
#include "types.h"
#include "replication.h"
#include <atomic>

using ondra_shared::Worker;

namespace sofadb {
using ondra_shared::Worker;

///Controls replication from one database to other
/** The replication works with two interfaces where first interface is used as source and seconds as target. */
class ReplicationTask {
public:
	using PProtocol = std::unique_ptr<IReplicationProtocol>;
	///Creates replication task
	/**
	 * @param source source database
	 * @param target target database
	 */
	ReplicationTask(PProtocol &&source, PProtocol &&target);
	virtual ~ReplicationTask();

	///Starts replication
	/**
	 * Replication processs can be started synchronously or asynchronously. It is heavly depend on how both replication protocols are
	 * implemented. It is better to expect, that replication is done asynchronously, however it can finish before the function returns. Also
	 * note blocking any overrided function can cause, that function will not return until the overrided function is unblocked
	 *
	 * @param since specifies sequence number where the replication starts. Specify 0 to replicate whole database. However if you
	 *   restarting replication to transfer latest changes, you can specify sequence number of previously replication to speed up the whole process
	 *
	 * @param filter specifies filter. The filter is applied on source database. It can contain name of script at the source database
	 * @param continuous set true to run continuous replication. Replication is running at background - asynchronously. Any change
	 * made in source database is immediatelly transfered to the target database
	 */
	void start(SeqNum since, json::Value filter, bool continuous);
	///Stops replication. Function blocks until replication is stopped
	void stop();
	///Determines, whether replication is running
	/**
	 * @retval true replication is running
	 * @return false replication is not running
	 */
	bool isRunning() const;

	///Retrieves last replicated sequence number
	/** While replication is running, the number is changing, so to receive more accurate value, stop the replication first (or wait until it
	 * is finished)
	 * @return last finished sequence number
	 */
	std::size_t getSeqNum() const {return seqnum;}

	enum class Side {
		source,target
	};

protected:
	///Called when replication is finished
	virtual void onFinish(SeqNum seqnum);
	///Called when error happen trying to put documents to the database
	/**
	 * @param doc document to put
	 * @param status status of put operation
	 * @retval true minor error - continue in replication
	 * @retval false major error - stop replication
	 */
	virtual bool onError(json::Value doc, PutStatus status);

	///Called after task is updated, not neceserily after each seqnum
	virtual void onUpdate(SeqNum seqnum);


	virtual void onWarning(Side side, int code, std::string &&text);
protected:

	PProtocol source, target;

	std::size_t initial_batch_size = 10;
	std::size_t batch_size = 1000;


	void doWork(std::size_t limit);
	bool checkStop();
	void finish();
	void retryWork();


	SeqNum seqnum;
	SeqNum old_seqnum;
	json::Value filter;
	bool continuous;
	ondra_shared::Countdown exitWait;
	using Grd = ondra_shared::CountdownGuard;

	std::atomic<bool> started;
	std::atomic<bool> stopSignal;
	std::atomic<int> async_detect;


};

} /* namespace sofadb */

#endif /* SRC_LIBSOFA_REPLICATIONTASK_H_ */
