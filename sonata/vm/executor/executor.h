#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct Executor Executor;

struct Executor
{
	Spinlock  lock;
	int       list_count;
	List      list;
	bool      wal_write;
	PlanGroup group;
	Router*   router;
	Db*       db;
};

void executor_init(Executor*, Db*, Router*);
void executor_free(Executor*);
void executor_enable_wal(Executor*, bool);
void executor_send(Executor*, Plan*, int, ReqList*);
void executor_recv(Executor*, Plan*, int);
void executor_complete(Executor*, Plan*);
void executor_commit(Executor*, Plan*);
