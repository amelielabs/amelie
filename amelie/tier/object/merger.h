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

typedef enum
{
	MERGER_SNAPSHOT,
	MERGER_MERGE
} MergerType;

struct MergerConfig
{
	MergerType type;
	Object*    origin;
	Heap*      heap;
	uint64_t*  id_seq;
	Keys*      keys;
	Source*    source;
};

struct Merger
{
	ObjectIterator object_iterator;
	HeapIterator   heap_iterator;
	MergeIterator  merge_iterator;
	Object*        a;
	Object*        b;
	Writer         writer;
};

void merger_init(Merger*);
void merger_free(Merger*);
void merger_reset(Merger*);
void merger_execute(Merger*, MergerConfig*);
