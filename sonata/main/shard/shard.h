#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct Shard Shard;

struct Shard
{
	int     order;
	TrxList prepared;
	Vm      vm;
	Node*   node;
	Task    task;
	List    link;
};

Shard*
shard_allocate(Node*, Db*, FunctionMgr*);
void shard_free(Shard*);
void shard_start(Shard*);
void shard_stop(Shard*);
