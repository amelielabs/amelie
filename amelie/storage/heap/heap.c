
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

#include <amelie_runtime>
#include <amelie_row.h>
#include <amelie_heap.h>
#include <valgrind/valgrind.h>

void
heap_init(Heap* self)
{
	self->buckets     = NULL;
	self->page_header = NULL;
	self->last        = NULL;
	self->header      = NULL;
	self->shadow      = NULL;
	page_mgr_init(&self->page_mgr);
}

void
heap_free(Heap* self)
{
	if (self->header)
		am_free(self->header);
	page_mgr_free(&self->page_mgr);
}

static inline void
heap_prepare(Heap* self, int id, int id_end, int size, int step)
{
	for (; id < id_end; id++)
	{
		auto bucket = &self->buckets[id];
		bucket->size        = size + step;
		bucket->list        = 0;
		bucket->list_offset = 0;
		bucket->list_count  = 0;
		bucket->id          = id;
		bucket->unused      = 0;
		size += step;
	}
}

void
heap_create(Heap* self)
{
	// prepare heap header

	// header + buckets[]
	auto size = sizeof(HeapHeader) + sizeof(HeapBucket) * 385;
	auto header = (HeapHeader*)am_malloc(size);
	header->count = 0;
	self->header  = header;
	self->buckets = header->buckets;

	// prepare buckets

	//
	// bucket  log    start     step
	//
	// 0       1-9    0         16
	// 64      10     1024      32
	// 96      11     2048      64
	// 128     12     4096      128
	// 160     13     8192      256
	// 192     14     16384     512
	// 224     15     32768     1024
	// 256     16     65536     2048
	// 288     17     131072    4096
	// 320     18     262144    8192
	// 352     19     524288    16384
	// 384     20     1048576   -
	//
	// /32 buckets per each log group (except 1-9)
	//
	heap_prepare(self, 0,   64,  0,       16);    // 64 buckets
	heap_prepare(self, 64,  96,  1024,    32);    // 32
	heap_prepare(self, 96,  128, 2048,    64);
	heap_prepare(self, 128, 160, 4096,    128);
	heap_prepare(self, 160, 192, 8192,    256);
	heap_prepare(self, 192, 224, 16384,   512);
	heap_prepare(self, 224, 256, 32768,   1024);
	heap_prepare(self, 256, 288, 65536,   2048);
	heap_prepare(self, 288, 320, 131072,  4096);
	heap_prepare(self, 320, 352, 262144,  8192);
	heap_prepare(self, 352, 384, 524288,  16384);
	heap_prepare(self, 384, 385, 1048576, 0);     // max
}

typedef struct
{
	int bucket;
	int step;
} HeapGroup;

static HeapGroup heap_log_to_bucket[20] =
{
	{ 0,   16    }, // 1
	{ 0,   16    }, // 2
	{ 0,   16    }, // 3
	{ 0,   16    }, // 4
	{ 0,   16    }, // 5
	{ 0,   16    }, // 6
	{ 0,   16    }, // 7
	{ 0,   16    }, // 8
	{ 0,   16    }, // 9
	{ 64,  32    }, // 10 1024+
	{ 96,  64    }, // 11
	{ 128, 128   }, // 12
	{ 160, 256   }, // 13
	{ 192, 512   }, // 14
	{ 224, 1024  }, // 15
	{ 256, 2048  }, // 16
	{ 288, 4096  }, // 17
	{ 320, 8192  }, // 18
	{ 352, 16384 }, // 19
	{ 384, 0     }, // 20 (max)
};

hot static inline HeapBucket*
heap_bucket_of(Heap* self, int size)
{
	assert(size <= 1*1024*1024);

	// calculate size log
	int log2 = (8 * sizeof(unsigned long long) - __builtin_clzl((size)) - 1);
	assert(log2 != 0);
	assert(log2 <= 20);

	// match log group
	auto group = &heap_log_to_bucket[log2 - 1];

	// match bucket by calculating the step align
	HeapBucket* bucket;
	if (likely(group->step))
	{
		int bucket_offset;
		if (log2 <= 9)
			bucket_offset = (size) / group->step;
		else
			bucket_offset = (size - (1 << log2)) / group->step;
		bucket = &self->buckets[group->bucket + bucket_offset];
	} else {
		// 20
		bucket = &self->buckets[group->bucket];
	}
	assert(bucket->size >= size);

	// todo: find first non-empty bucket (using bitmaps)
	return bucket;
}

hot void*
heap_allocate(Heap* self, int size)
{
	// use shadow heap during snapshot
	Heap* heap;
	if (self->shadow)
		heap = self->shadow;
	else
		heap = self;

	// match bucket by size
	auto bucket = heap_bucket_of(heap, sizeof(HeapChunk) + size);

	// get chunk from the free list
	HeapChunk* chunk;
	if (likely(bucket->list_count > 0))
	{
		auto page = (int)bucket->list;
		auto page_offset = (int)bucket->list_offset;
		chunk = page_mgr_pointer_of(&heap->page_mgr, page, page_offset);
		assert(chunk->bucket == bucket->id);
		assert(chunk->is_free);
		bucket->list        = chunk->prev;
		bucket->list_offset = chunk->prev_offset;
		bucket->list_count--;
	} else
	{
		// use current or create new page
		auto page_header = heap->page_header;
		if (unlikely(!heap->last ||
		             (uint32_t)(heap->page_mgr.page_size - page_header->size) <
		                        bucket->size))
		{
			auto page = page_mgr_allocate(&heap->page_mgr);
			page_header = (PageHeader*)page->pointer;
			page_header->size    = sizeof(PageHeader);
			page_header->order   = heap->page_mgr.list_count - 1;
			page_header->last    = 0;
			page_header->padding = 0;
			heap->page_header = page_header;
			heap->last = NULL;
			heap->header->count++;
		}
		chunk = (HeapChunk*)((uintptr_t)page_header + page_header->size);
		chunk->tsn     = 0;
		chunk->offset  = page_header->size;
		chunk->bucket  = bucket->id;
		chunk->is_last = true;
		chunk->padding = 0;
		if (likely(heap->last))
		{
			chunk->bucket_left  = heap->last->bucket;
			heap->last->is_last = false;
		} else {
			chunk->bucket_left = 0;
		}
		heap->last = chunk;
		page_header->last = page_header->size;
		page_header->size += bucket->size;
	}
	chunk->prev             = 0;
	chunk->prev_offset      = 0;
	chunk->is_free          = false;
	chunk->is_shadow        = self->shadow != NULL;
	chunk->is_shadow_delete = false;

	assert(misalign_of(chunk->data) == 0);
	return chunk->data;
}

hot void
heap_release(Heap* self, void* pointer)
{
	auto chunk = heap_chunk_of(pointer);
	assert(! chunk->is_free);

	// snapshot
	//
	// collect all deletes of main heap into shadow heap
	//
	if (self->shadow && !chunk->is_shadow)
	{
		auto ptr = heap_allocate(self->shadow, sizeof(void*));
		memcpy(ptr, &pointer, sizeof(void*));
		heap_chunk_of(ptr)->is_shadow = true;
		heap_chunk_of(ptr)->is_shadow_delete = true;
		return;
	}

	// add chunk to the free list
	auto bucket = &self->buckets[chunk->bucket];
	chunk->tsn          = 0;
	chunk->prev         = bucket->list;
	chunk->prev_offset  = bucket->list_offset;
	chunk->is_free      = true;
	bucket->list        = heap_page_of(chunk)->order;
	bucket->list_offset = chunk->offset;
	bucket->list_count++;

	// if not first, merge left  if left->free (use chunk->bucket_left to match)
	// if not last,  merge right if right->free

	// todo: bitmap
}

void
heap_snapshot(Heap* self, Heap* shadow)
{
	assert(! self->shadow);
	self->shadow = shadow;
}
