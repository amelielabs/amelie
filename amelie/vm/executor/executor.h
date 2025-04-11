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
	Spinlock lock;
	uint64_t id;
	int      list_count;
	List     list;
	Prepare  prepare;
	CoreMgr* core_mgr;
	Db*      db;
};

void executor_init(Executor*, Db*, CoreMgr*);
void executor_free(Executor*);
void executor_send(Executor*, Dtr*, int, ReqList*);
void executor_recv(Executor*, Dtr*, int);
void executor_commit(Executor*, Dtr*, Buf*);
