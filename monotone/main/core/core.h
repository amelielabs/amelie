#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Core Core;

struct Core
{
	Share        share;
	// vm
	FunctionMgr  function_mgr;
	// db
	Db           db;
	// cluster
	ReqMap       req_map;
	ReqLock      req_lock;
	HubMgr       hub_mgr;
	ShardMgr     shard_mgr;
	// server
	Server       server;
	UserMgr      user_mgr;
	// config state
	CatalogMgr   catalog_mgr;
	ConfigState  config_state;
	Control      control;
};

Core*
core_create(void);
void core_free(Core*);
void core_start(Core*, bool);
void core_stop(Core*);
void core_main(Core*);
