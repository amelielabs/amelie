#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct Share Share;

struct Share
{
	Executor*    executor;
	Router*      router;
	Cluster*     cluster;
	FunctionMgr* function_mgr;
	Db*          db;
};
