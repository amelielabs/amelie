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

typedef struct Refresh Refresh;

struct Refresh
{
	ServiceLock lock;
	Part*       origin;
	Heap*       origin_heap;
	Table*      table;
	List        parts;
	int         parts_count;
	Volume*     volume;
	Merger      merger;
	Storage*    storage;
};

void refresh_init(Refresh*, Storage*);
void refresh_free(Refresh*);
void refresh_reset(Refresh*);
void refresh_run(Refresh*, Id*, Str*);
