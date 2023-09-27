#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Shard Shard;

struct Shard
{
	int          order;
	StorageList  storage_list;
	ShardConfig* config;
	Task         task;
};

Shard*
shard_allocate(ShardConfig*);
void shard_free(Shard*);
void shard_start(Shard*);
void shard_stop(Shard*);
