#include <rpc/rpcServer.h>
#include <shared/logOutput.h>
#include <shared/stdLogFile.h>
#include <shared/stringview.h>
#include <simpleServer/abstractService.h>
#include <simpleServer/address.h>
#include <simpleServer/asyncProvider.h>
#include <simpleServer/http_server.h>
#include <simpleServer/realpath.h>
#include <simpleServer/threadPoolAsync.h>
#include <iostream>
#include <leveldb/db.h>
#include <leveldb/env.h>
#include "config.h"
#include "../libsofa/kvapi_leveldb.h"
#include "../libsofa/databasecore.h"
#include "../libsofa/docdb.h"
#include "../libsofa/systemdbs.h"
#include "rpcapi.h"

using doxyhub::ServerConfig;
using ondra_shared::LogLevel;
using ondra_shared::StdLogFile;
using ondra_shared::StrViewA;
using simpleServer::ArgList;
using simpleServer::AsyncProvider;
using simpleServer::MiniHttpServer;
using simpleServer::NetAddr;
using simpleServer::RpcHttpServer;
using simpleServer::ServiceControl;
using simpleServer::ThreadPoolAsync;

class LevelDBLogger: public leveldb::Logger {
public:

	LevelDBLogger():logobj("LEVELDB") {}

	virtual void Logv(const char* format, va_list ap) {
		char buff[4096];
		vsnprintf(buff,sizeof(buff),format, ap);
		logobj.debug("$1", buff);
	}

	ondra_shared::LogObject logobj;
};


int main(int argc, char **argv) {

	return ServiceControl::create(argc, argv, "sofadb",
			[=](ServiceControl control, StrViewA , ArgList args){


		if (args.length < 1) {
			throw std::runtime_error("You need to supply a pathname of configuration");
		}

		std::string cfgpath = realpath(args[0]);
		ServerConfig cfg;
		cfg.parse(cfgpath);

		StdLogFile::create(cfg.logfile, cfg.loglevel, LogLevel::debug)->setDefault();

		auto logger = std::make_unique<LevelDBLogger>();
		cfg.dbopts.info_log = logger.get();

		sofadb::PKeyValueDatabase kvdb = sofadb::leveldb_open(cfg.dbopts,cfg.datapath);
		sofadb::SofaDB db(kvdb);


		AsyncProvider asyncProvider = ThreadPoolAsync::create(cfg.server_threads, cfg.server_dispatchers);
		NetAddr addr = NetAddr::create(cfg.bind,8800,NetAddr::IPvAll);


		RpcHttpServer serverObj(addr, asyncProvider);
		RpcHttpServer::Config scfg;
		scfg.enableConsole = cfg.rpc_enable_console;
		scfg.enableDirect = cfg.rpc_enable_tcp;
		scfg.enableWS = cfg.rpc_enable_ws;
		scfg.maxReqSize = cfg.http_maxreqsize;


		serverObj.addRPCPath("/RPC", scfg);
		serverObj.add_ping();
		serverObj.add_listMethods();

		sofadb::RpcAPI rpcApi(db);
		rpcApi.init(serverObj);

		serverObj.start();

		control.dispatch();

		return 0;
	});


}
