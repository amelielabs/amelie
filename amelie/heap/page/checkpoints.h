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

typedef struct CheckpointsRef CheckpointsRef;
typedef struct Checkpoints    Checkpoints;

struct CheckpointsRef
{
	uint64_t id;
	int      refs;
	List     link;
};

struct Checkpoints
{
	Spinlock        lock;
	CheckpointsRef* current;
	List            list;
	int             list_count;
	Uuid            id;
};

void checkpoints_init(Checkpoints*, Uuid*);
void checkpoints_free(Checkpoints*);
void checkpoints_open(Checkpoints*, uint64_t);
void checkpoints_gc(Checkpoints*, uint64_t);
CheckpointsRef*
checkpoints_add(Checkpoints*, uint64_t);
