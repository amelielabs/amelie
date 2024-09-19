#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct System System;

struct System
{
	Share       share;
	// vm
	FunctionMgr function_mgr;
	UdfContext  udf_ctx;
	// repl
	Repl        repl;
	// executor
	Executor    executor;
	Cluster     cluster;
	FrontendMgr frontend_mgr;
	bool        lock;
	RpcQueue    lock_queue;
	// db
	Db          db;
	// server
	Server      server;
	UserMgr     user_mgr;
	// config state
	Control     control;
};

System*
system_create(void);
void system_free(System*);
void system_start(System*, bool);
void system_stop(System*);
void system_main(System*);
