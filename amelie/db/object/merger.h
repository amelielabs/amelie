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

typedef struct MergerConfig MergerConfig;
typedef struct Merger       Merger;

struct MergerConfig
{
	uint64_t  lsn;
	Object*   origin;
	Heap*     heap;
	Keys*     keys;
	Encoding* encoding;
	Storage*  storage;
};

struct Merger
{
	List           objects;
	int            objects_count;
	Writer*        writers;
	Writer*        writers_cache;
	Buf            heap_index;
	ObjectIterator object_iterator;
	MergeIterator  merge_iterator;
};

void merger_init(Merger*);
void merger_free(Merger*);
void merger_reset(Merger*);
void merger_execute(Merger*, MergerConfig*);

static inline Object*
merger_first(Merger* self)
{
	if (! self->objects_count)
		return NULL;
	return container_of(list_first(&self->objects), Object, link);
}
