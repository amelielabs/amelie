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

typedef struct HeapBucket HeapBucket;
typedef struct HeapHeader HeapHeader;
typedef struct Heap       Heap;

struct HeapBucket
{
	// 8 bytes
	uint64_t size:        16;
	uint64_t list:        19;
	uint64_t list_offset: 19;
	uint64_t id:           8;
	uint64_t unused:       2;
} packed;

struct HeapHeader
{
	uint64_t   free[4];
	uint32_t   used_count;
	uint64_t   used;
	HeapBucket buckets[];
} packed;

struct Heap
{
	HeapBucket* buckets;
	HeapHeader* header;
	Storage     storage;
};

always_inline static inline Row*
heap_at(Heap* self, int page, int offset)
{
	return (Row*)storage_pointer_of(&self->storage, page, offset);
}

always_inline static inline Page*
heap_page_of(Row* self)
{
	return (Page*)((uintptr_t)self - self->offset);
}

Heap*  heap_allocate(void);
void   heap_free(Heap*);
size_t heap_create(Heap*, char*);
size_t heap_open(Heap*, char*);
Row*   heap_add(Heap*, int);
void   heap_remove(Heap*, Row*);
