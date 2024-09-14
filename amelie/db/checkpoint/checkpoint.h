#pragma once

//
// amelie.
//
// Real-Time SQL Database.
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
