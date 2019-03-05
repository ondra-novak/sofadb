/*
 * eventrouter.cpp
 *
 *  Created on: 10. 2. 2019
 *      Author: ondra
 */

#include <libsofa/eventrouter.h>
#include <shared/logOutput.h>

namespace sofadb {

EventRouter::EventRouter(Worker worker):worker(worker),schrev(0) {

}


void EventRouter::receiveEvent(DatabaseCore::ObserverEvent event,
		Handle h, SeqNum seqnum) {

	Sync _(lock);
	snm[h] = seqnum;
	auto b = oph.lower_bound(OPHKey(h,0));
	auto e = oph.lower_bound(OPHKey(h+1,0));
	while (b != e) {
		WaitHandle wh = b->second;
		auto i = omap.find(wh);
		if (i != omap.end()) {
			worker >> [fn = std::move(i->second.observer),g=CntSync(cntd)] {
				fn(true);
			};
		}
		omap.erase(i);
		b = oph.erase(b);
	}
	for (auto &&c : globlist) {
		worker >> [event,h,seqnum,fn = GlobalObserver(c),g=CntSync(cntd)] {
			fn(event,h,seqnum);
		};
	}
	if (event == DatabaseCore::event_close) {
		snm.erase(h);
	}
	reschedule();

}

DatabaseCore::Observer EventRouter::createObserver() {
	RefCntPtr<EventRouter> ptr(this);
	return [ptr](DatabaseCore::ObserverEvent event, Handle h, SeqNum num) {
		ptr->receiveEvent(event,h,num);
	};
}

EventRouter::WaitHandle EventRouter::waitForEvent(Handle db, SeqNum since, std::size_t timeout, Observer&& observer) {

	Sync _(lock);

	std::size_t tmp = 1;
	auto snmiter = snm.find(db);
	if (snmiter == snm.end() || snmiter->second > since) {
		worker >> [observer = std::move(observer)] {
			observer(true);
		};
	} else {

		TimePoint tm = TimePoint::clock::now() + std::chrono::milliseconds(timeout);
		tmp = std::chrono::duration_cast<std::chrono::milliseconds>(tm.time_since_epoch()).count();
		while (omap.find(tmp) != omap.end())
			tmp++;

		Reg &r = omap[tmp];
		r.db = db;
		r.observer = std::move(observer);

		oph.insert(OPHKey(db, tmp));
	}
	reschedule();
	return tmp;
}

bool EventRouter::cancelWait(WaitHandle wh, bool notify_fn) {
	Sync _(lock);
	auto i = omap.find(wh);
	if (i == omap.end()) return false;

	if (notify_fn) {
		worker >> [observer = std::move(i->second.observer)] {
			observer(false);
		};
	}

	oph.erase(OPHKey(i->second.db, wh));
	omap.erase(i);
	reschedule();
	return true;
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

EventRouter::~EventRouter() {
	stop();
}


void EventRouter::reschedule() {
	using namespace ondra_shared;

	if (schevent) {
		schevent->notify_all();
	}

	auto i = omap.begin();

	TimePoint now = TimePoint::clock::now();
	std::size_t nmili = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
	auto e = omap.lower_bound(nmili);
	while (i != e) {
		worker >> [fn = std::move(i->second.observer),g=CntSync(cntd)] {
			fn(false);
		};
		oph.erase(OPHKey(i->second.db, i->first));
		i = omap.erase(i);
	}
	std::uint64_t rev = ++schrev;
	if (i == omap.end()) return;

	TimePoint until = now+std::chrono::milliseconds(i->first - nmili);
	worker >> [this,until,rev,g=CntSync(cntd)] {

		Sync _(lock);
		if (rev != schrev) return;

		std::condition_variable cond;
		schevent = &cond;
		auto waitres = cond.wait_until(_,until);
		schevent = nullptr;
		if (waitres == std::cv_status::timeout) {
			reschedule();
		}
	};


}

void EventRouter::stop() {

	{
		Sync _(lock);

		globlist.clear();
		oph.clear();
		omap.clear();
		snm.clear();
		reschedule();
	}
	cntd.wait();

}


} /* namespace sofadb */

