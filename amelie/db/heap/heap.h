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
typedef struct ChunkEnd   ChunkEnd;
typedef struct HeapBucket HeapBucket;
typedef struct HeapGroup  HeapGroup;
typedef struct Heap       Heap;

struct Chunk
{
	uint64_t next: 19;
	uint64_t next_offset: 21;
	uint64_t prev: 19;
	uint64_t prev_offset: 21;
	uint64_t size: 20;
	uint64_t bucket: 9;
	uint64_t free: 1;
	uint64_t unused: 18;
	uint8_t  data[];
} packed;

struct ChunkEnd
{
	uint16_t bucket: 9;
	uint16_t free: 1;
	uint16_t unused: 6;
} packed;

struct HeapBucket
{
	uint64_t size: 24;
	uint64_t list: 19;
	uint64_t list_offset: 21;
	uint64_t list_count: 40;
	uint64_t id: 8;
	uint64_t unused: 16;
} packed;

struct HeapGroup
{
	int bucket;
	int step;
};

struct Heap
{
	HeapBucket* buckets;
	Page*       page;
	PageMgr     page_mgr;
};

always_inline static inline Chunk*
chunk_of(void* pointer)
{
	return (Chunk*)((uintptr_t)pointer - sizeof(Chunk));
}

always_inline static inline ChunkEnd*
chunk_end_of(Chunk* self, HeapBucket* bucket)
{
	return (ChunkEnd*)(((uintptr_t)self + bucket->size) - sizeof(ChunkEnd));
}

void  heap_init(Heap*);
void  heap_free(Heap*);
void  heap_create(Heap*);
void* heap_allocate(Heap*, int);
void  heap_release(Heap*, void*);
