#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Share Share;

struct Share
{
	MetaMgr*    meta_mgr;
	TableMgr*   table_mgr;
	StorageMgr* storage_mgr;
	Wal*        wal;
	Db*         db;
	ShardMap*   shard_map;
	ShardMgr*   shard_mgr;
	ReqLock*    req_lock;
	ReqCache*   req_cache;
	Lock*       cat_lock;
};
