#pragma once

//
// sonata.
//
// SQL Database for JSON.
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
	CheckpointWorker* workers;
	int               workers_count;
	int               rr;
};

void checkpoint_init(Checkpoint*);
void checkpoint_free(Checkpoint*);
void checkpoint_prepare(Checkpoint*, int);
void checkpoint_add(Checkpoint*, StorageMgr*);
void checkpoint_run(Checkpoint*);
void checkpoint_wait(Checkpoint*);
