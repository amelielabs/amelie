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
	RequestList  prepared;
	ShardConfig* config;
	Vm           vm;
	Task         task;
};

Shard*
shard_allocate(ShardConfig*, Db*, FunctionMgr*);
void shard_free(Shard*);
void shard_start(Shard*);
void shard_stop(Shard*);
