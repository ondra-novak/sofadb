/*
 * eventrouter.cpp
 *
 *  Created on: 10. 2. 2019
 *      Author: ondra
 */

#include <libsofa/eventrouter.h>

namespace sofadb {

EventRouter::EventRouter(Worker worker):worker(worker) {

}

void EventRouter::receiveEvent(DatabaseCore::ObserverEvent event,
		Handle h, SeqNum seqnum) {

	Sync _(lock);
	Listener &lst = hmap[h];
	for (auto &&c : lst.olist) {
		worker >> c;
	}
	lst.olist.clear();
	for (auto &&c : globlist) {
		worker >> [event,h,seqnum,fn = GlobalObserver(c)] {
			fn(event,h,seqnum);
		};
	}
	if (event == DatabaseCore::event_close) {
		hmap.erase(h);
	}

}

DatabaseCore::Observer EventRouter::createObserver() {
	RefCntPtr<EventRouter> ptr(this);
	return [ptr](DatabaseCore::ObserverEvent event, Handle h, SeqNum num) {
		ptr->receiveEvent(event,h,num);
	};
}

bool EventRouter::waitForEvent(Handle db, SeqNum since,
				Observer&& observer, WaitHandle* handle) {

	if (handle) *handle = observer.target<void>();
	Sync _(lock);

	auto iter = hmap.find(db);
	if (iter == hmap.end() || iter->second.lastSeqNum > since) return false;
	iter->second.olist.push_back(std::move(observer));
	return true;
}

bool EventRouter::cancelWait(Handle db, WaitHandle wh) {
	Sync _(lock);
	auto iter = hmap.find(db);
	if (iter == hmap.end()) return false;
	ObserverList &olist = iter->second.olist;
	for (auto i = olist.begin(); i!=olist.end(); ++iter) {
		if (i->target<void>() == wh) {
			olist.erase(i);
			return true;
		}
	}
	return false;
}

EventRouter::ObserverHandle EventRouter::registerObserver(GlobalObserver&& observer) {
	ObserverHandle h = observer.target<void>();
	Sync _(lock);
	globlist.push_back(std::move(observer));
	return h;
}

bool EventRouter::removeObserver(ObserverHandle wh) {
	Sync _(lock);
	for  (auto iter = globlist.begin(); iter != globlist.end(); ++iter) {
		if (iter->target<void>() == wh) {
			globlist.erase(iter);
			return true;
		}
	}
	return false;
}

} /* namespace sofadb */
