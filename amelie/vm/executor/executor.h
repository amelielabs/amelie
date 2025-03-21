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
	int      list_count;
	List     list;
	/*Commit   commit;*/
	Db*      db;
};

void executor_init(Executor*, Db*);
void executor_free(Executor*);
void executor_send(Executor*, Dtr*, int, JobList*);
void executor_recv(Executor*, Dtr*, int);
void executor_commit(Executor*, Dtr*, Buf*);
