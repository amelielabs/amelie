#pragma once

//
// sonata.
//
// SQL Database for JSON.
//

typedef struct Share Share;

struct Share
{
	Executor*    executor;
	Router*      router;
	ShardMgr*    shard_mgr;
	FunctionMgr* function_mgr;
	// wal
	Db*          db;
};
