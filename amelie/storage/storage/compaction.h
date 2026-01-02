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

typedef struct Storage    Storage;
typedef struct Compaction Compaction;

struct Compaction
{
	Storage* storage;
	Task     task;
};

Compaction*
compaction_allocate(Storage*);
void compaction_free(Compaction*);
void compaction_start(Compaction*);
void compaction_stop(Compaction*);
