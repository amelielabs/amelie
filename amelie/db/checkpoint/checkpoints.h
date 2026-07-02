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

typedef struct CheckpointRef CheckpointRef;
typedef struct Checkpoints   Checkpoints;

struct CheckpointRef
{
	uint64_t id;
	int      refs;
	List     link;
};

struct Checkpoints
{
	Spinlock       lock;
	CheckpointRef* current;
	List           list;
	int            list_count;
	Catalog*       catalog;
};

void checkpoints_init(Checkpoints*, Catalog*);
void checkpoints_free(Checkpoints*);
void checkpoints_open(Checkpoints*);
void checkpoints_gc(Checkpoints*);
void checkpoints_add(Checkpoints*, uint64_t);
CheckpointRef*
checkpoints_ref(Checkpoints*);
void checkpoints_unref(Checkpoints*, CheckpointRef*);
void checkpoints_list(CheckpointRef*, Buf*);
