#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct Executor Executor;

struct Executor
{
	Spinlock lock;
	int      list_count;
	List     list;
	Commit   commit;
	Router*  router;
	Db*      db;
};

void executor_init(Executor*, Db*, Router*);
void executor_free(Executor*);
void executor_send(Executor*, Dtr*, int, ReqList*);
void executor_recv(Executor*, Dtr*, int);
void executor_commit(Executor*, Dtr*, Buf*);
