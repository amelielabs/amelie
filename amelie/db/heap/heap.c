
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

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_json.h>
#include <amelie_config.h>
#include <amelie_row.h>
#include <amelie_heap.h>
#include <valgrind/valgrind.h>

void
heap_init(Heap* self)
{
	self->buckets = NULL;
	self->page    = NULL;
	page_mgr_init(&self->page_mgr);
}

void
heap_free(Heap* self)
{
	if (self->buckets)
		am_free(self->buckets);
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
	self->page = page_mgr_allocate(&self->page_mgr);

	// init buckets
	self->buckets = am_malloc(sizeof(HeapBucket) * 385);

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
	heap_prepare(self, 256, 288, 131072,  4096);
	heap_prepare(self, 288, 320, 262144,  8192);
	heap_prepare(self, 352, 384, 524288,  16384);
	heap_prepare(self, 384, 385, 1048576, 0);     // max
}

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
	auto group  = &heap_log_to_bucket[log2 - 1];

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
	// match bucket by size
	auto bucket = heap_bucket_of(self, sizeof(Chunk) + size + sizeof(ChunkEnd));

	// get chunk from the free list
	Chunk* chunk;
	if (likely(bucket->list_count > 0))
	{
		auto page = (int)bucket->list;
		auto page_offset = (int)bucket->list_offset;
		chunk = page_mgr_pointer_of(&self->page_mgr, page, page_offset);
		assert(chunk->bucket == bucket->id);
		assert(chunk->free);
		bucket->list        = chunk->next;
		bucket->list_offset = chunk->next_offset;
		bucket->list_count--;
		// set the reference link to self
		chunk->next        = page;
		chunk->next_offset = page_offset;
	} else
	{
		// use current or create new page
		if (unlikely((uint32_t)(self->page_mgr.page_size - self->page->pos) < bucket->size))
			self->page = page_mgr_allocate(&self->page_mgr);
		auto page = self->page;
		chunk = (Chunk*)(page->pointer + page->pos);
		chunk->next        = self->page_mgr.list_count - 1;
		chunk->next_offset = page->pos;
		chunk->prev        = 0;
		chunk->prev_offset = 0;
		chunk->size        = bucket->size;
		chunk->unused      = 0;
		page->pos += bucket->size;
	}
	chunk->bucket = bucket->id;
	chunk->free   = false;

	auto chunk_end = chunk_end_of(chunk, bucket);
	chunk_end->bucket = bucket->id;
	chunk_end->free   = false;

	assert(align_of(chunk->data) == 0);
	return chunk->data;
}

hot void
heap_release(Heap* self, void* pointer)
{
	auto chunk = chunk_of(pointer);
	assert(chunk == page_mgr_pointer_of(&self->page_mgr, chunk->next, chunk->next_offset));
	assert(! chunk->free);

	auto bucket      = &self->buckets[chunk->bucket];
	auto head        = (int)chunk->next;

	auto head_offset = (int)chunk->next_offset;
	chunk->next         = bucket->list;
	chunk->next_offset  = bucket->list_offset;
	chunk->free         = true;
	bucket->list        = head;
	bucket->list_offset = head_offset;
	bucket->list_count++;

	auto chunk_end = chunk_end_of(chunk, bucket);
	assert(! chunk_end->free);
	assert(chunk_end->bucket == chunk->bucket);
	chunk_end->free = true;

	// todo: bitmap
}
