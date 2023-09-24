#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Shard Shard;

struct Shard
{
	StorageList  storages;
	ShardConfig* config;
	Task         task;
};

Shard*
shard_allocate(ShardConfig*);
void shard_free(Shard*);
void shard_start(Shard*);
void shard_stop(Shard*);
