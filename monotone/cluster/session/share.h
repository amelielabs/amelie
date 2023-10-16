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
	ReqMap*     req_map;
	ReqLock*    req_lock;
	ReqCache*   req_cache;
	ShardMgr*   shard_mgr;
	Lock*       cat_lock;
};
