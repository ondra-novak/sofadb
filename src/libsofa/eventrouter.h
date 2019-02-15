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
#include "databasecore.h"
#include <shared/worker.h>
#include <shared/refcnt.h>


namespace sofadb {

using ondra_shared::Worker;

///Routes database events to various observers
/** It is executed in separate thread(s). There can be many observers for many databases
 * Every observer is registered only for one-shot. When event is emited, all observers
 * are notified and removed from the list. They need to register again.
 */
class EventRouter: public RefCntObj {

public:
	typedef DatabaseCore::Handle Handle;
	typedef std::function<void()> Observer;
	typedef std::vector<Observer> ObserverList;
	typedef std::function<void(DatabaseCore::ObserverEvent, Handle, SeqNum)> GlobalObserver;
	typedef std::vector<GlobalObserver> GlobalObserverList;
	typedef const void *WaitHandle;
	typedef const void *ObserverHandle;

	///EventRouter need Worker as backend. Worker defines threads
	EventRouter(Worker worker);


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
	/**
	 * @param db database handle
	 * @param since last known sequence id for the observer
	 * @param observer observer object
	 * @param handle if set, receives handle of the registration. It can be used to cancel registration
	 * @retval true registered and waiting
	 * @retval false can't register, because there are already changes to process
	 */
	bool waitForEvent(Handle db, SeqNum since, Observer &&observer, WaitHandle *handle = nullptr);

	///Cancels waiting
	/**
	 * @param db database handle
	 * @param wh wait handle
	 * @retval false not found, probably already executed
	 * @retval true canceled
	 */
	bool cancelWait(Handle db, WaitHandle wh);


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


protected:


	struct Listener {
		ObserverList olist;
		SeqNum lastSeqNum = 0;
	};

	typedef std::unordered_map<Handle, Listener> HandleMap;
	typedef std::unique_lock<std::recursive_mutex> Sync;
	HandleMap hmap;
	GlobalObserverList globlist;
	Worker worker;
	std::recursive_mutex lock;

};

using PEventRouter = RefCntPtr<EventRouter>;

} /* namespace sofadb */

#endif /* SRC_LIBSOFA_EVENTROUTER_H_ */
