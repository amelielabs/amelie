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

typedef struct HeapChunk  HeapChunk;
typedef struct HeapBucket HeapBucket;
typedef struct HeapHeader HeapHeader;
typedef struct Heap       Heap;

struct HeapChunk
{
	// 16 bytes

	// previous version (38 bits)
	uint64_t prev: 19;
	uint64_t prev_offset: 19;

	// reserve (38 bits)
	uint64_t reserved: 38;

	// chunk
	uint64_t offset: 19;
	uint64_t bucket: 8;
	uint64_t is_free: 1;

	// unused
	uint64_t padding: 24;

	// row data
	uint8_t  data[];
} packed;

struct HeapBucket
{
	// 8 bytes
	uint64_t size: 16;
	uint64_t list: 19;
	uint64_t list_offset: 19;
	uint64_t id: 8;
	uint64_t unused: 2;
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

always_inline static inline HeapChunk*
heap_chunk_of(void* pointer)
{
	return (HeapChunk*)((uintptr_t)pointer - sizeof(HeapChunk));
}

always_inline static inline HeapChunk*
heap_chunk_at(Heap* self, int page, int offset)
{
	return (HeapChunk*)storage_pointer_of(&self->storage, page, offset);
}

always_inline static inline Page*
heap_page_of(HeapChunk* self)
{
	return (Page*)((uintptr_t)self - self->offset);
}

Heap*  heap_allocate(void);
void   heap_free(Heap*);
size_t heap_create(Heap*, char*);
size_t heap_open(Heap*, char*);
void*  heap_add(Heap*, int);
void   heap_remove(Heap*, void*);
