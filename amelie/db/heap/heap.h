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
	// 24 bytes

	// transaction id (64 bits)
	uint64_t tsn;

	// previous version (40 bits)
	uint64_t prev: 19;
	uint64_t prev_offset: 21;

	// chunk (41 bits)
	uint64_t offset: 21;
	uint64_t bucket: 9;
	uint64_t bucket_left: 9;
	uint64_t is_free: 1;
	uint64_t is_last: 1;
	uint64_t is_shadow: 1;
	uint64_t is_shadow_free: 1;
	uint64_t padding: 45;

	// row data
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

#define HEAP_MAGIC   0x20849610
#define HEAP_VERSION 0

struct HeapHeader
{
	uint32_t   crc;
	uint32_t   magic;
	uint32_t   version;
	uint16_t   hash_min;
	uint16_t   hash_max;
	uint8_t    compression;
	uint8_t    encryption;
	uint32_t   count;
	HeapBucket buckets[];
} packed;

struct Heap
{
	HeapBucket* buckets;
	PageHeader* page_header;
	HeapChunk*  last;
	HeapHeader* header;
	Heap*       shadow;
	bool        shadow_free;
	PageMgr     page_mgr;
};

always_inline static inline HeapChunk*
heap_chunk_of(void* pointer)
{
	return (HeapChunk*)((uintptr_t)pointer - sizeof(HeapChunk));
}

static inline PageHeader*
heap_page_of(HeapChunk* self)
{
	return (PageHeader*)((uintptr_t)self - self->offset);
}

static inline HeapChunk*
heap_first(Heap* self)
{
	if (unlikely(! self->last))
		return NULL;
	return (HeapChunk*)((uintptr_t)self->page_header + sizeof(PageHeader));
}

Heap* heap_allocate(void);
void  heap_free(Heap*);
void* heap_add(Heap*, int);
void  heap_remove(Heap*, void*);
void  heap_snapshot(Heap*, Heap*, bool);

static inline bool
heap_snapshot_has(Heap* self)
{
	return self->shadow != NULL;
}
