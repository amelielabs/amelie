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

	// chunk (34 + 18 bits)
	uint64_t offset: 19;
	uint64_t bucket: 9;
	uint64_t is_free: 1;
	uint64_t is_last: 1;
	uint64_t is_shadow: 1;
	uint64_t is_shadow_free: 1;
	uint64_t is_shadow_prev: 1;
	uint64_t is_evicted: 1;

	// unused
	uint64_t padding: 18;

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
	uint64_t id: 9;
	uint64_t unused: 15;
} packed;

#define HEAP_MAGIC   0x20849610
#define HEAP_VERSION 0

struct HeapHeader
{
	uint32_t   crc;
	uint32_t   magic;
	uint32_t   version;
	uint64_t   lsn;
	uint64_t   tsn;
	uint16_t   hash_min;
	uint16_t   hash_max;
	uint8_t    compression;
	uint32_t   count;
	uint32_t   count_used;
	uint64_t   size_used;
	HeapBucket buckets[];
} packed;

struct Heap
{
	HeapBucket* buckets;
	PageHeader* page_header;
	HeapHeader* header;
	HeapChunk*  last;
	Heap*       shadow;
	bool        shadow_free;
	PageMgr     page_mgr;
};

always_inline static inline HeapChunk*
heap_chunk_of(void* pointer)
{
	return (HeapChunk*)((uintptr_t)pointer - sizeof(HeapChunk));
}

always_inline static inline HeapChunk*
heap_chunk_at(Heap* self, int page, int offset)
{
	return (HeapChunk*)page_mgr_pointer_of(&self->page_mgr, page, offset);
}

static inline PageHeader*
heap_page_of(HeapChunk* self)
{
	return (PageHeader*)((uintptr_t)self - self->offset);
}

Heap* heap_allocate(void);
void  heap_free(Heap*);
void* heap_add(Heap*, int);
void  heap_remove(Heap*, void*);
void  heap_snapshot(Heap*, Heap*, bool);

static inline void
heap_follow(Heap* self, uint64_t tsn)
{
	if (self->shadow)
		self = self->shadow;
	if (tsn > self->header->tsn)
		self->header->tsn = tsn;
}
