/*
 * replicator.cpp
 *
 *  Created on: 3. 3. 2019
 *      Author: ondra
 */

#include <shared/logOutput.h>
#include "replicationserver.h"
#include "document.h"
#include "replicator.h"

using ondra_shared::logInfo;

namespace sofadb {
using namespace json;
using ondra_shared::logInfo;
using ondra_shared::LogObject;

Replicator::Replicator(DocumentDB &db, const PEventRouter &rt, ExternalReplicationFactory &&erf)
	:db(db)
	,rt(rt)
	,erf(std::move(erf)) {

	DatabaseCore &core = db.getDBCore();
	h = core.getHandle("_replicator");
	if (h == DatabaseCore::invalid_handle) {
		h = core.create("_replicator",Storage::permanent);
		DatabaseCore::DBConfig cfg;
		cfg.history_max_age = 0;
		cfg.history_max_count = 0;
		cfg.history_min_count = 5;
		cfg.history_max_deleted = 0;
		cfg.logsize = 5;
		core.setConfig(h,cfg);
	}
	dispatchRequests();
}
Replicator::~Replicator() {
	Sync _(lock);
	std::condition_variable_any exitcond;
	this->exitWait = &exitcond;
	rt->cancelWait(wh,true);
	exitcond.wait(_);
}

void Replicator::dispatchRequests() {

	wh = rt->waitForEvent(h,since, 24*60*60*1000, [=](bool){
		worker();
	});
}

void Replicator::workerExit() {
	exitWait->notify_all();
}

void Replicator::worker() {
	since = db.readChanges(h,since,false,OutputFormat::data_and_deleted,[&](Value doc) {
		update(doc);
		return true;

	});
	Sync _(lock);
	if (exitWait) {
		workerExit();
	} else {
		bootstrap = false;
		dispatchRequests();
	}
}

static void setError(Document &doc, int code, std::string_view text) {
	doc.set("error", Object
			("code", code)
			("message",StrViewA(text)))
		   ("running", false);
}


class Replicator::MyTask: public ReplicationTask  {
public:
	MyTask(Replicator &owner, std::string name, PProtocol &&source, PProtocol &&target)
			:ReplicationTask(std::move(source), std::move(target))
			 , owner(owner),name(name),logObj("Replication "+name) {
		logObj.info("started");

	}
	~MyTask() {
		logObj.info("exiting");
	}

	virtual void onFinish(SeqNum seqnum) {
		owner.onFinish(name,seqnum);
		logObj.info("finished");
	}

	virtual void onUpdate(SeqNum seqnum) {
		owner.onUpdate(name,seqnum);
		logObj.info("update $1", seqnum);
	}
	virtual void onWarning(Side side, int code, std::string &&msg) {
		owner.onWarning(name,side,code,std::move(msg));
		logObj.info("warning $1 $2 $3", static_cast<int>(side),code,msg);
	}

protected:
	Replicator &owner;
	std::string name;
	LogObject logObj;
};

void Replicator::update(Document doc) {
	Value dummy;


	try {

		std::string name (doc.getID());
		auto iter = replMap.find(name);
		if (doc.getDeleted()) {
			if (iter != replMap.end()) {

				PReplication &r = iter->second;
				if (r->isRunning()) r->stop();
				replMap.erase(iter);
			}
		} else {
			bool enabled = doc["enabled"].getBool();
			bool can_run = !doc["running"].defined();
			bool should_run = doc["running"].getBool();
			bool bootstrap = doc["bootstrap"].getBool() && this->bootstrap;
			if (bootstrap) can_run = should_run = true;
			if (iter == replMap.end()) {
				if (!enabled || !(can_run || should_run)) return;
				PReplication task ( createTask(name, doc) );
				if (task == nullptr) {
					db.client_put(h, doc, dummy);
					return ;
				}
				iter = replMap.insert(std::pair(name, std::move(task))).first;
			}
			PReplication &task = iter->second;
			bool running = task->isRunning();
			if ((can_run || (should_run && !running)) && enabled) {
				doc.set("running", true);
				db.client_put(h,doc,dummy);
				if (!running) {
					Value filter = doc["filter"];
					SeqNum since = bootstrap?0LL:doc["since"].getUInt();
					bool continuous = doc["continuous"].getBool();
					task->start(since,filter,continuous);
				}
			} else if (running && !enabled) {
				task->stop();
				SeqNum seqnum = task->getSeqNum();
				doc.set("since", seqnum);
				doc.set("running", false);
				db.client_put(h,doc, dummy);
				replMap.erase(iter);
			}
		}
	}
	catch (std::exception &e) {
		setError(doc,500,e.what());
		db.client_put(h,doc,dummy);
	}


}

IReplicationProtocol *Replicator::createProtocol(json::Value def) {
	String name = def.toString();
	DatabaseCore &core = db.getDBCore();
	Handle hdb = core.getHandle(name.str());
	if (hdb == core.invalid_handle) {
		if (erf == nullptr) return nullptr;
		return erf(def);
	}
	return new ReplicationServer(db, rt, hdb);
}


Replicator::PReplication Replicator::createTask(const std::string &name,Document &doc) {
	Value source = doc["source"];
	Value target = doc["target"];

	MyTask::PProtocol psrc ( createProtocol(source) );
	MyTask::PProtocol ptrg ( createProtocol(target) );
	if (psrc == nullptr) {
		setError(doc, 1, "Unable to open source");return nullptr;
	}
	if (ptrg == nullptr) {
		setError(doc, 2, "Unable to open target");return nullptr;
	}
	return PReplication ( new MyTask(*this,name,std::move(psrc), std::move(ptrg)) );

}

void Replicator::onFinish(const std::string& name, SeqNum seqnum) {
	Value vdoc = db.get(h,name,OutputFormat::data);
	if (vdoc != nullptr) {
		Value tmp;
		Document doc(vdoc);
		doc.set("running",false);
		doc.set("since", seqnum);
		db.client_put(h,doc,tmp);
	}

	Sync _(lock);
	replMap.erase(name);
}

void Replicator::onUpdate(const std::string& name, SeqNum seqnum) {
	Value vdoc = db.get(h,name,OutputFormat::data);
	if (vdoc != nullptr) {
		Value tmp;
		Document doc(vdoc);
		doc.set("running",true);
		doc.set("since", seqnum);
		db.client_put(h,doc,tmp);
	}
}

void Replicator::onWarning(const std::string& name, ReplicationTask::Side side, int code,
		std::string&& msg) {
	Value vdoc = db.get(h,name,OutputFormat::data);
	if (vdoc != nullptr) {
		Value tmp;
		Document doc(vdoc);{
			auto w = doc.object("warning");
			auto o = w.object(side==ReplicationTask::Side::source?"source":"target");
			o.set("code", code);
			o.set("message", msg);
		}
		db.client_put(h,doc,tmp);
	}
}

} /* namespace sofadb */
