#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct ShardMgr ShardMgr;

struct ShardMgr
{
	Shard**      shards;
	int          shards_count;
	FunctionMgr* function_mgr;
	Db*          db;
};

void shard_mgr_init(ShardMgr*, Db*, FunctionMgr*);
void shard_mgr_free(ShardMgr*);
void shard_mgr_open(ShardMgr*);
void shard_mgr_create(ShardMgr*, int);
void shard_mgr_set_partition_map(ShardMgr*, Router*);
void shard_mgr_start(ShardMgr*);
void shard_mgr_stop(ShardMgr*);
void shard_mgr_recover(ShardMgr*);
