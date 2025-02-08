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
	Repl*        repl;
	FrontendMgr* frontend_mgr;
	BackendMgr*  backend_mgr;
	FunctionMgr* function_mgr;
	UserMgr*     user_mgr;
	Db*          db;
};
