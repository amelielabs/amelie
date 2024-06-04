#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct CheckpointWorker CheckpointWorker;
typedef struct Checkpoint       Checkpoint;

struct CheckpointWorker
{
	int        list_count;
	List       list;
	Condition* on_complete;
	pid_t      pid;
};

struct Checkpoint
{
	Buf*              catalog;
	uint64_t          lsn;
	CheckpointWorker* workers;
	int               workers_count;
	int               rr;
};

void checkpoint_init(Checkpoint*);
void checkpoint_free(Checkpoint*);
void checkpoint_begin(Checkpoint*, CatalogMgr*, uint64_t, int);
void checkpoint_add(Checkpoint*, PartList*);
void checkpoint_run(Checkpoint*);
void checkpoint_wait(Checkpoint*, CheckpointMgr*);
