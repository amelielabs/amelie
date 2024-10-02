#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct Compute Compute;

struct Compute
{
	Vm      vm;
	TrList  prepared;
	TrCache cache;
	Node*   node;
	Task    task;
	List    link;
};

Compute*
compute_allocate(Node*, Db*, FunctionMgr*);
void compute_free(Compute*);
void compute_start(Compute*);
void compute_stop(Compute*);
void compute_sync(Compute*);
