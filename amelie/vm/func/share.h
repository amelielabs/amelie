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
	Commit*      commit;
	CoreMgr*     core_mgr;
	Repl*        repl;
	FunctionMgr* function_mgr;
	UserMgr*     user_mgr;
	Db*          db;
};

#define share() ((Share*)am_share)
