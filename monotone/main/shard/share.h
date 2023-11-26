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
	SchemaMgr*    schema_mgr;
	ViewMgr*      view_mgr;
	TableMgr*     table_mgr;
	PartMgr*      part_mgr;
	Wal*          wal;
	Db*           db;
	Router*       router;
	DispatchLock* dispatch_lock;
	ReqCache*     req_cache;
	ShardMgr*     shard_mgr;
	Lock*         cat_lock;
};
