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

typedef struct CheckpointWorker CheckpointWorker;
typedef struct Checkpoint       Checkpoint;

struct CheckpointWorker
{
	int    list_count;
	List   list;
	Notify notify;
	Event  on_complete;
	pid_t  pid;
};

struct Checkpoint
{
	Buf*              catalog;
	uint64_t          lsn;
	CheckpointWorker* workers;
	int               workers_count;
	int               rr;
	CheckpointMgr*    mgr;
};

void checkpoint_init(Checkpoint*, CheckpointMgr*);
void checkpoint_free(Checkpoint*);
void checkpoint_begin(Checkpoint*, uint64_t, int);
void checkpoint_add(Checkpoint*, Part*);
void checkpoint_run(Checkpoint*);
void checkpoint_wait(Checkpoint*);
