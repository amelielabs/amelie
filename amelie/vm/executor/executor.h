#pragma once

//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// Copyright (c) 2024 Amelie Labs.
//
// AGPL-3.0 Licensed.
//

typedef struct Executor Executor;

struct Executor
{
	Mutex    lock;
	CondVar  cond_var;
	bool     shutdown;
	uint64_t seq;
	int      list_count;
	List     list;
	int      pending_count;
	List     pending;
	Commit   commit;
	ReqMgr   req_mgr;
	Db*      db;
};

void executor_init(Executor*, Db*);
void executor_free(Executor*);
void executor_shutdown(Executor*);
void executor_send(Executor*, Dtr*, int, ReqList*);
void executor_recv(Executor*, Dtr*, int);
void executor_commit(Executor*, Dtr*, Buf*);
Req* executor_next(Executor*);
void executor_complete(Executor*, Req*);
