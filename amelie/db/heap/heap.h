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

typedef struct Chunk      Chunk;
typedef struct HeapBucket HeapBucket;
typedef struct HeapHeader HeapHeader;
typedef struct HeapGroup  HeapGroup;
typedef struct Heap       Heap;

struct Chunk
{
	// 16 bytes
	uint64_t next: 19;
	uint64_t next_offset: 21;
	uint64_t prev: 19;
	uint64_t prev_offset: 21;
	uint64_t size: 20;
	uint64_t bucket_left: 9;
	uint64_t bucket: 9;
	uint64_t free: 1;
	uint64_t last: 1;
	uint64_t unused: 8;
	uint8_t  data[];
} packed;

struct HeapBucket
{
	// 16 bytes
	uint64_t size: 24;
	uint64_t list: 19;
	uint64_t list_offset: 21;
	uint64_t list_count: 40;
	uint64_t id: 8;
	uint64_t unused: 16;
} packed;

struct HeapHeader
{
	uint32_t   crc;
	uint64_t   lsn;
	uint32_t   count;
	HeapBucket buckets[];
} packed;

struct HeapGroup
{
	int bucket;
	int step;
};

struct Heap
{
	HeapBucket* buckets;
	PageHeader* page_header;
	Chunk*      last;
	HeapHeader* header;
	PageMgr     page_mgr;
};

void  heap_init(Heap*);
void  heap_free(Heap*);
void  heap_create(Heap*);
void* heap_allocate(Heap*, int);
void  heap_release(Heap*, void*);

always_inline static inline Chunk*
chunk_of(void* pointer)
{
	return (Chunk*)((uintptr_t)pointer - sizeof(Chunk));
}

static inline Chunk*
heap_first(Heap* self)
{
	if (unlikely(! self->last))
		return NULL;
	return (Chunk*)((uintptr_t)self->page_header + sizeof(PageHeader));
}
