/*
 * config.cpp
 *
 *  Created on: 14. 6. 2018
 *      Author: ondra
 */


#include "config.h"
#include <simpleServer/exceptions.h>
#include <shared/virtualMember.h>
#include <shared/stringview.h>
#include <shared/ini_config.h>
#include <stdexcept>

namespace doxyhub {


using ondra_shared::IniConfig;


void ServerConfig::parse(const std::string& name) {
	IniConfig cfg;
	cfg.load(name,[](auto &&item){
		if (item.key == "include") {
			throw std::runtime_error("Config_parse - cannot open file: "+item.data);
		}
	});


	const IniConfig::KeyValueMap &log = cfg["log"];
	logfile = log.mandatory["file"].getPath();
	loglevel = log.mandatory["level"].getString();

	const IniConfig::KeyValueMap &server = cfg["server"];
	bind = server.mandatory["listen"].getString();
	server_threads = server.mandatory["threads"].getUInt();
	server_dispatchers = server.mandatory["dispatchers"].getUInt();
	http_maxreqsize = server.mandatory["max_req_size"].getUInt();

	const IniConfig::KeyValueMap &rpc = cfg["rpc"];
	rpc_enable_console = rpc["console"].getBool(true);
	rpc_enable_tcp = rpc["tcp"].getBool(true);
	rpc_enable_ws = rpc["ws"].getBool(true);


	const IniConfig::KeyValueMap &database = cfg["database"];
	datapath = database.mandatory["path"].getPath();
	IniConfig::Value v = database["cache"];
	if (v.defined()) dbopts.block_cache = (cacheptr = std::shared_ptr<leveldb::Cache>(leveldb::NewLRUCache(v.getUInt()))).get();
	v = database["block_restart_interval"];
	if (v.defined()) dbopts.block_restart_interval = v.getUInt();
	v = database["block_size"];
	if (v.defined()) dbopts.block_size = v.getUInt();
	v = database["create_if_missing"];
	if (v.defined()) dbopts.create_if_missing = v.getBool();
					else dbopts.create_if_missing = true;
	v = database["max_file_size"];
	if (v.defined()) dbopts.max_file_size = v.getUInt();
	v = database["max_open_files"];
	if (v.defined()) dbopts.max_open_files = v.getUInt();
	v = database["paranoid_checks"];
	if (v.defined()) dbopts.paranoid_checks = v.getBool();
	v = database["write_buffer_size"];
	if (v.defined()) dbopts.write_buffer_size = v.getUInt();
	v = database["bloom_bits"];
	filterptr = std::shared_ptr<leveldb::FilterPolicy>(const_cast<leveldb::FilterPolicy *>(leveldb::NewBloomFilterPolicy(v.getUInt(10))));
	dbopts.filter_policy = filterptr.get();

}


} /* namespace doxyhub */

