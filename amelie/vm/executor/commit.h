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
	Batch     batch;
	DtrQueue  queue;
	Executor* executor;
	CoreMgr*  core_mgr;
	Storage*  storage;
	Task      task;
};

void commit_init(Commit*, Storage*, CoreMgr*, Executor*);
void commit_free(Commit*);
void commit_start(Commit*);
void commit_stop(Commit*);
void commit(Commit*, Dtr*, Buf*);
