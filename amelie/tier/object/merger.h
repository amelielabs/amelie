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
	MERGER_REFRESH,
	MERGER_SPLIT
} MergerType;

struct MergerConfig
{
	MergerType type;
	bool       origin_hash;
	Object*    origin;
	Heap*      heap;
	Keys*      keys;
	Source*    source;
};

struct Merger
{
	List           objects;
	int            objects_count;
	List           writers;
	List           writers_cache;
	Hasher         hasher;
	ObjectIterator object_iterator;
	HeapIterator   heap_iterator;
	MergeIterator  merge_iterator;
};

void merger_init(Merger*);
void merger_free(Merger*);
void merger_reset(Merger*);
void merger_execute(Merger*, MergerConfig*);
