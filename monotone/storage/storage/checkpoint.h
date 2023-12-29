#pragma once

//
// indigo
//
// SQL OLTP database
//

typedef struct Checkpoint Checkpoint;

struct Checkpoint
{
	uint64_t   lsn;
	int        list_count;
	int        list_count_skipped;
	List       list;
	Condition* on_complete;
	pid_t      pid;
};

void checkpoint_init(Checkpoint*, uint64_t);
void checkpoint_free(Checkpoint*);
void checkpoint_add(Checkpoint*, StorageMgr*);
void checkpoint_run(Checkpoint*);
void checkpoint_wait(Checkpoint*);
