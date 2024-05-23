#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct System System;

struct System
{
	Share       share;
	// vm
	FunctionMgr function_mgr;
	// db
	Db          db;
	// cluster
	Executor    executor;
	Router      router;
	ShardMgr    shard_mgr;
	FrontendMgr frontend_mgr;
	// server
	UserMgr     user_mgr;
	Server      server;
	// config state
	CatalogMgr  catalog_mgr;
	Control     control;
};

System*
system_create(void);
void system_free(System*);
void system_start(System*, bool);
void system_stop(System*);
void system_main(System*);
