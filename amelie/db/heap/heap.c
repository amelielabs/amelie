
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
#include <amelie_transaction.h>
#include <amelie_storage.h>
#include <amelie_flat.h>
#include <amelie_heap.h>

static inline void
heap_set(Heap* self, int id, int id_end, int size, int step)
{
	for (; id < id_end; id++)
	{
		auto bucket = &self->buckets[id];
		bucket->size        = size + step;
		bucket->list        = 0;
		bucket->list_offset = 0;
		bucket->id          = id;
		bucket->unused      = 0;
		size += step;
	}
}

static void
heap_prepare(Heap* self)
{
	if (self->header)
		return;

	// prepare heap header

	// header + buckets[]
	auto size = sizeof(HeapHeader) + sizeof(HeapBucket) * 256;
	auto header = (HeapHeader*)am_malloc(size);
	header->count      = 0;
	header->used_count = 0;
	header->used       = 0;

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
	// 256     16     65536     -
	//
	// /32 buckets per each log group (except 1-9)
	//
	heap_set(self, 0,   64,  0,     16);   // 64 buckets
	heap_set(self, 64,  96,  1024,  32);   // 32
	heap_set(self, 96,  128, 2048,  64);
	heap_set(self, 128, 160, 4096,  128);
	heap_set(self, 160, 192, 8192,  256);
	heap_set(self, 192, 224, 16384, 512);
	heap_set(self, 224, 256, 32768, 1024); // max 64k
}

Heap*
heap_allocate(void)
{
	auto self = (Heap*)am_malloc(sizeof(Heap));
	self->buckets = NULL;
	self->header  = NULL;
	storage_init(&self->storage, STORAGE_HEAP, 512 * 1024);
	heap_prepare(self);
	return self;
}

void
heap_free(Heap* self)
{
	if (self->header)
		am_free(self->header);
	storage_free(&self->storage);
	am_free(self);
}

size_t
heap_create(Heap* self, char* path)
{
	// create heap storage file
	auto size = sizeof(HeapHeader) + sizeof(HeapBucket) * 256;
	return storage_create(&self->storage, path, (uint8_t*)self->header, size);
}

size_t
heap_open(Heap* self, char* path)
{
	// read heap storage file
	Buf meta;
	buf_init(&meta);
	defer_buf(&meta);
	auto size_file = storage_open(&self->storage, path, STORAGE_HEAP, &meta);

	// validate heap header size
	int size = sizeof(HeapHeader) + sizeof(HeapBucket) * 256;
	if (unlikely(buf_size(&meta) != size))
		error("storage: file '{str}' has invalid heap header", path);

	// rewrite header
	memcpy(self->header, meta.start, size);
	return size_file;
}

typedef struct
{
	int bucket;
	int step;
} HeapGroup;

static HeapGroup heap_log_to_bucket[16] =
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
	{ 256, 0     }, // 16 (max)
};

hot static inline HeapBucket*
heap_bucket_of(Heap* self, int size)
{
	// 64KB max row (including chunk)
	assert(size <= (int)((64l * 1024) + sizeof(HeapChunk)));

	// log2(10) below
	if (likely(size < 1024))
		return &self->buckets[size >> 4]; // 16

	auto log2 = (8 * sizeof(unsigned long long) - __builtin_clzl((size)) - 1);

	// log(16) max
	if (unlikely(log2 >= 16))
		return &self->buckets[255];

	// match the log group
	auto group = &heap_log_to_bucket[log2 - 1];

	// match the bucket
	auto offset = (size - (1 << log2)) / group->step;
	return &self->buckets[group->bucket + offset];
}

hot void*
heap_add(Heap* self, int size)
{
	// match bucket by size
	auto bucket = heap_bucket_of(self, sizeof(HeapChunk) + size);

	// get chunk from the free list
	HeapChunk* chunk;
	if (likely(bucket->list_offset > 0))
	{
		auto page_id     = (int)bucket->list;
		auto page_offset = (int)bucket->list_offset;
		chunk = storage_pointer_of(&self->storage, page_id, page_offset);
		assert(chunk->bucket == bucket->id);
		assert(chunk->is_free);
		bucket->list        = chunk->prev;
		bucket->list_offset = chunk->prev_offset;
	} else
	{
		// ensure current page fit the bucket
		if (storage_ensure(&self->storage, bucket->size))
			self->header->count++;

		auto page = self->storage.current;
		chunk = (HeapChunk*)page_at(page, page->position);
		chunk->offset  = page->position;
		chunk->bucket  = bucket->id;
		chunk->padding = 0;
		page->position += bucket->size;

	}
	chunk->prev        = 0;
	chunk->prev_offset = 0;
	chunk->reserved    = 0;
	chunk->is_free     = false;

	// update total used metrics
	self->header->used_count++;
	self->header->used += bucket->size;

	assert(misalign_of(chunk->data) == 0);
	return chunk->data;
}

hot void
heap_remove(Heap* self, void* pointer)
{
	auto chunk = heap_chunk_of(pointer);
	assert(! chunk->is_free);

	// add chunk to the free list
	auto bucket = &self->buckets[chunk->bucket];
	chunk->prev           = bucket->list;
	chunk->prev_offset    = bucket->list_offset;
	chunk->is_free        = true;
	bucket->list          = heap_page_of(chunk)->id;
	bucket->list_offset   = chunk->offset;

	// todo: update bitmap

	// update total used metrics
	self->header->used_count--;
	self->header->used -= bucket->size;
}
