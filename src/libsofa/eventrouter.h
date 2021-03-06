/*
 * eventrouter.h
 *
 *  Created on: 10. 2. 2019
 *      Author: ondra
 */

#ifndef SRC_LIBSOFA_EVENTROUTER_H_
#define SRC_LIBSOFA_EVENTROUTER_H_

#include <functional>
#include <list>
#include <unordered_map>
#include <map>
#include <set>
#include "databasecore.h"
#include <shared/worker.h>
#include <shared/refcnt.h>
#include <shared/scheduler.h>
#include <shared/waitableEvent.h>
#include <condition_variable>




namespace sofadb {

using ondra_shared::Worker;
using ondra_shared::WaitableEvent;


///Routes database events to various observers
/** It is executed in separate thread(s). There can be many observers for many databases
 * Every observer is registered only for one-shot. When event is emited, all observers
 * are notified and removed from the list. They need to register again.
 */
class EventRouter: public RefCntObj {

public:
	///observer
	/**
	 * @param bool true - change detected, false - timeout
	 */
	typedef std::function<void(bool)> Observer;
	typedef DatabaseCore::Handle Handle;
	typedef std::chrono::time_point<std::chrono::steady_clock> TimePoint;
	typedef std::size_t WaitHandle;

	struct Reg{
		Handle db;
		Observer observer;
	};

	typedef std::map<WaitHandle, Reg> ObserverMap;
	typedef std::pair<Handle, WaitHandle> OPHKey;
	typedef std::set<OPHKey> ObserverPerHandle;
	typedef std::map<Handle, SeqNum> SeqNumMap;


	typedef std::function<void(DatabaseCore::ObserverEvent, Handle, SeqNum)> GlobalObserver;
	typedef std::vector<GlobalObserver> GlobalObserverList;
	typedef const void *ObserverHandle;

	///EventRouter need Worker as backend. Worker defines threads
	EventRouter(Worker worker);


	~EventRouter();

	///Called for new event from the database
	/**
	 *
	 * @param event event type
	 * @param h handle of database
	 * @param seqnum sequence number of the event
	 */
	void receiveEvent(DatabaseCore::ObserverEvent event, Handle h, SeqNum seqnum);

	///Creates observer for DabaseCore
	DatabaseCore::Observer createObserver();

	///Registers observer
	WaitHandle  waitForEvent(Handle db, SeqNum since, std::size_t timeout, Observer &&observer);

	///Cancels waiting
	/**
	 * @param wh handle returned by waitForEvent function
	 * @param notify_fn set true if you need to notify observer about cancelation. This
	 * causes, that observer will be executed with false as argument. Note that
	 * execution is queued to the worker and provided asynchronously
	 *
	 * @retval true success
	 * @retval false not found
	 */
	bool cancelWait(WaitHandle wh, bool notify_fn = false);


	///registers global event observer
	/** This allows to listen all events in the database
	 *
	 * @param observer observer
	 * @return handle, which can be used to removeObserver
	 *
	 * Because it would be possible to lost event while the observer is processing preceding event,
	 * the observer is not registered for one shot. You need to remove observer when you need
	 * to stop receiving the events.
	 *
	 * @see removeObserver
	 */
	ObserverHandle registerObserver(GlobalObserver &&observer);

	///Removes global observer
	/**
	 * @param wh handle received by registerObserver
	 * @retval true removed
	 * @retval false not found
	 */
	bool removeObserver(ObserverHandle wh);

	template<typename Fn>
	bool listDBs(Fn &&fn) {
		std::unique_lock<std::mutex> _(lock);
		for (auto &x: snm)
			if (!fn(x)) return false;
		return true;
	}

	bool getLastSeqNum(Handle h, SeqNum &sn) {
		std::unique_lock<std::mutex> _(lock);
		auto iter = snm.find(h);
		if (iter == snm.end()) return false;
		sn = iter->second;
		return true;
	}

	void stop();

	template<typename Fn>
	void dispatch(Fn &&fn);

protected:


	ObserverMap omap;
	ObserverPerHandle oph;
	SeqNumMap snm;

	typedef std::unique_lock<std::mutex> Sync;
	typedef ondra_shared::CountdownGuard CntSync;

	GlobalObserverList globlist;
	Worker worker;
	std::mutex lock;
	ondra_shared::Countdown cntd;


	std::condition_variable *schevent = nullptr;
	std::atomic<std::uintptr_t>  schrev;
	void exitAll();

	void reschedule();

};

template<typename Fn>
void EventRouter::dispatch(Fn &&fn) {
	worker >> std::move(fn);
	Sync _(lock);
	reschedule();
}

using PEventRouter = RefCntPtr<EventRouter>;

} /* namespace sofadb */

#endif /* SRC_LIBSOFA_EVENTROUTER_H_ */
