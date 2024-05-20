#pragma once

//
// sonata.
//
// SQL Database for JSON.
//

typedef struct Executor Executor;

struct Executor
{
	Spinlock  lock;
	int       list_count;
	List      list;
	PlanGroup group;
	Router*   router;
};

void executor_init(Executor*, Router*);
void executor_free(Executor*);
void executor_create(Executor*);
void executor_send(Executor*, Plan*, int, ReqList*);
void executor_recv(Executor*, Plan*, int);
void executor_complete(Executor*, Plan*);
void executor_commit(Executor*, Plan*);
