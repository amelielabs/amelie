#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct System System;

struct System
{
	Share        share;
	// vm
	FunctionMgr  function_mgr;
	// db
	Db           db;
	// cluster
	Router       router;
	DispatchLock dispatch_lock;
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

System*
system_create(void);
void system_free(System*);
void system_start(System*, bool);
void system_stop(System*);
void system_main(System*);
