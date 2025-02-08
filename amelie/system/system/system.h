#pragma once

//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// Copyright (c) 2024 Amelie Labs.
//
// AGPL-3.0 Licensed.
//

typedef struct System System;

struct System
{
	Share       share;
	// vm
	FunctionMgr function_mgr;
	// repl
	Repl        repl;
	// executor
	Executor    executor;
	FrontendMgr frontend_mgr;
	BackendMgr  backend_mgr;
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
