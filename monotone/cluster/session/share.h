#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Share Share;

struct Share
{
	MetaMgr*      meta_mgr;
	TableMgr*     table_mgr;
	StorageMgr*   storage_mgr;
	Wal*          wal;
	ShardMap*     shard_map;
	ShardMgr*     shard_mgr;
	RequestSched* req_sched;
	Lock*         cat_lock;
};
