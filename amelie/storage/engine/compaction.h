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

typedef struct Compaction Compaction;

struct Compaction
{
	Refresh refresh;
	Engine* engine;
	Task    task;
};

Compaction*
compaction_allocate(Engine*);
void compaction_free(Compaction*);
void compaction_start(Compaction*);
void compaction_stop(Compaction*);
