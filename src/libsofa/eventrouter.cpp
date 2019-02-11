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

	RefCntPtr<EventRouter> ptr(this);
	worker >> [ptr,h,event, seqnum] {

		ObserverList el;
		{
			Sync _(ptr->lock);
			Listener &lst = ptr->hmap[h];
			std::swap(lst.olist,el);
			if (event == DatabaseCore::event_close) {
				ptr->hmap.erase(h);
			}
			else {
				lst.lastSeqNum = seqnum;
				lst.olist.reserve(el.size());
			}
		}
		for (auto &&x: el) {
			x();
		}
	};

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
	for (std::size_t i = 0, cnt = iter->second.olist.size(); i < cnt; i++) {
		if (iter->second.olist[i].target<void>() == wh) {
			iter->second.olist.erase(iter->second.olist.begin()+i);
			return true;
		}
	}
	return false;
}

} /* namespace sofadb */
