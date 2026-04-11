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

typedef struct Commit Commit;

struct Commit
{
	Executor* executor;
	Db*       db;
	Pub*      pub;
	Task      task;
};

void commit_init(Commit*, Pub*, Db*, Executor*);
void commit_free(Commit*);
void commit_start(Commit*);
void commit_stop(Commit*);
void commit(Commit*, Dtr*, Buf*);
