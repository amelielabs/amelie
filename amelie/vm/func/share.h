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

typedef struct Share Share;

struct Share
{
	Executor*    executor;
	CoreMgr*     core_mgr;
	Repl*        repl;
	FunctionMgr* function_mgr;
	UserMgr*     user_mgr;
	Db*          db;
	RecoverIf*   recover_if;
	void*        recover_if_arg;
};
