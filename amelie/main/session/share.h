#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct Share      Share;
typedef struct SessionMgr SessionMgr;

struct Share
{
	Executor*    executor;
	Repl*        repl;
	Cluster*     cluster;
	FunctionMgr* function_mgr;
	SessionMgr*  session_mgr;
	UserMgr*     user_mgr;
	Db*          db;
};
