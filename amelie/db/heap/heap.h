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

	// next version (40 bits)
	uint64_t next: 19;
	uint64_t next_offset: 21;

	// chunk (43 + 5 bits)
	uint64_t offset: 21;
	uint64_t bucket: 9;
	uint64_t bucket_left: 9;
	uint64_t is_free: 1;
	uint64_t is_last: 1;
	uint64_t is_shadow: 1;
	uint64_t is_shadow_free: 1;
	uint64_t padding: 5;

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

	// lru
	uint64_t   lru: 19;
	uint64_t   lru_offset: 21;
	uint64_t   lru_tail: 19;
	uint64_t   lru_tail_offset: 21;

	// meta
	uint64_t   lsn;
	uint16_t   hash_min;
	uint16_t   hash_max;
	uint8_t    compression;
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
	bool        lru;
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

Heap* heap_allocate(bool);
void  heap_free(Heap*);
void* heap_add(Heap*, int);
void  heap_remove(Heap*, void*);
void  heap_snapshot(Heap*, Heap*, bool);

hot always_inline static inline void
heap_push(Heap* self, HeapChunk* chunk)
{
	// push chunk to the head of the lru list
	auto     header      = self->header;
	uint32_t head        = header->lru;
	uint32_t head_offset = header->lru_offset;

	// chunk->next = head
	chunk->prev        = 0;
	chunk->prev_offset = 0;
	chunk->next        = head;
	chunk->next_offset = head_offset;

	auto chunk_page = heap_page_of(chunk)->order;
	if (likely(head_offset))
	{
		// head->prev = chunk
		auto head_chunk = heap_chunk_at(self, head, head_offset);
		head_chunk->prev        = chunk_page;
		head_chunk->prev_offset = chunk->offset;
	} else
	{
		// tail = chunk
		header->lru_tail        = chunk_page;
		header->lru_tail_offset = chunk->offset;
	}

	// head = chunk
	header->lru        = chunk_page;
	header->lru_offset = chunk->offset;
}

hot always_inline static inline void
heap_unlink(Heap* self, HeapChunk* chunk)
{
	// chunk->prev->next = chunk->next
	if (chunk->prev_offset)
	{
		auto prev = heap_chunk_at(self, chunk->prev, chunk->prev_offset);
		prev->next        = chunk->next;
		prev->next_offset = chunk->next_offset;
	}

	// chunk->next->prev = chunk->prev
	if (chunk->next_offset)
	{
		auto next = heap_chunk_at(self, chunk->next, chunk->next_offset);
		next->prev        = chunk->prev;
		next->prev_offset = chunk->prev_offset;
	}

	// replace head (chunk is a head)
	auto chunk_page = heap_page_of(chunk)->order;
	auto header     = self->header;
	if (header->lru == chunk_page &&
	    header->lru_offset == chunk->offset)
	{
		// head = chunk->next
		header->lru        = chunk->next;
		header->lru_offset = chunk->next_offset;
	}

	// replace tail (chunk is a tail)
	if (header->lru_tail == chunk_page &&
	    header->lru_tail_offset == chunk->offset)
	{
		// tail = chunk->prev
		header->lru_tail        = chunk->prev;
		header->lru_tail_offset = chunk->prev_offset;
	}
}

hot always_inline static inline void
heap_up(Heap* self, void* pointer)
{
	if (! self->lru)
		return;
	// move to the top of the lru list
	auto chunk = heap_chunk_of(pointer);
	heap_unlink(self, chunk);
	heap_push(self, chunk);
}
