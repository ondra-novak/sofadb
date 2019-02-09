/*
 * config.h
 *
 *  Created on: 14. 6. 2018
 *      Author: ondra
 */

#ifndef SRC_BUILDER_CONFIG_H_
#define SRC_BUILDER_CONFIG_H_
#include <istream>
#include <memory>
#include <string>
#include <leveldb/cache.h>
#include <leveldb/options.h>
#include <leveldb/filter_policy.h>

namespace doxyhub {


class ServerConfig {
public:

	std::string logfile;
	std::string loglevel;

	int server_threads;
	int server_dispatchers;
	std::string bind;

	std::string datapath;

	bool rpc_enable_console;
	bool rpc_enable_tcp;
	bool rpc_enable_ws;
	std::size_t http_maxreqsize;


	leveldb::Options dbopts;

	std::shared_ptr<leveldb::Cache> cacheptr;
	std::shared_ptr<leveldb::FilterPolicy> filterptr;

	void parse(const std::string &name);

protected:



};

} /* namespace doxyhub */

#endif /* SRC_BUILDER_CONFIG_H_ */
