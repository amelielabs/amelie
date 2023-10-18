#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Share Share;

struct Share
{
	FunctionMgr*  function_mgr;
	MetaMgr*      meta_mgr;
	TableMgr*     table_mgr;
	StorageMgr*   storage_mgr;
	Wal*          wal;
	Db*           db;
	Router*       router;
	DispatchLock* dispatch_lock;
	ReqCache*     req_cache;
	ShardMgr*     shard_mgr;
	Lock*         cat_lock;
};
