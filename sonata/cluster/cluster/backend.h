#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct Backend Backend;

struct Backend
{
	int     order;
	TrxList prepared;
	Vm      vm;
	Node*   node;
	Task    task;
	List    link;
};

Backend*
backend_allocate(Node*, Db*, FunctionMgr*);
void backend_free(Backend*);
void backend_start(Backend*);
void backend_stop(Backend*);
