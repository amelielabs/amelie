#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct Shard Shard;

struct Shard
{
	int          order;
	TrxList      prepared;
	Vm           vm;
	ShardConfig* config;
	Task         task;
};

Shard*
shard_allocate(ShardConfig*, Db*, FunctionMgr*);
void shard_free(Shard*);
void shard_start(Shard*);
void shard_stop(Shard*);
