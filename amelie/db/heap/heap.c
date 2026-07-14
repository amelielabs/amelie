
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
#include <amelie_type.h>
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
	memset(header->free, 0, sizeof(header->free));
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

always_inline static inline void
heap_bucket_set(Heap* self, HeapBucket* bucket)
{
	// mark bucket as having free rows
	const int id = bucket->id;
	self->header->free[id >> 6] |= (1ULL << (id & 63));
}

always_inline static inline void
heap_bucket_unset(Heap* self, HeapBucket* bucket)
{
	// mark bucket as empty
	const int id = bucket->id;
	self->header->free[id >> 6] &= ~(1ULL << (id & 63));
}

hot static inline HeapBucket*
heap_match_after(Heap* self, int id)
{
	// search for a next free row in the larger buckets
	const auto bitmaps    = self->header->free;
	auto       bitmap_pos = id >> 6;  // / 64
	auto       bit        = id  & 63; // % 64

	// mask smaller buckets in the current bitmap
	uint64_t mask = bitmaps[bitmap_pos] & ~((1ULL << bit) - 1);
	if (mask != 0)
	{
		auto id = (bitmap_pos << 6) + __builtin_ctzll(mask);
		return &self->buckets[id];
	}

	// look for the rest of the buckets
	for (auto i = bitmap_pos + 1; i < 4; i++)
	{
		if (! bitmaps[i])
			continue;
		auto id = (i << 6) + __builtin_ctzll(bitmaps[i]);
		return &self->buckets[id];
	}

	return NULL;
}

hot static inline HeapBucket*
heap_match(Heap* self, int size)
{
	// 64KB max row (including row)
	assert(size <= (int)(64 * 1024));

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
	auto id = group->bucket + offset;

	// return if the bucket has free rows
	auto bucket = &self->buckets[id];
	if (likely(bucket->list_offset > 0))
		return bucket;

	// search for a next free row in the larger buckets
	auto bucket_next = heap_match_after(self, id);
	if (bucket_next)
		return bucket_next;

	// no free rows found, allocate a new one
	return bucket;
}

hot Row*
heap_add(Heap* self, int size)
{
	// match next free bucket (size includes Row)
	auto bucket = heap_match(self, size);

	// get a row from the bucket free list
	Row* row;
	if (likely(bucket->list_offset > 0))
	{
		row = heap_at(self, bucket->list, bucket->list_offset);
		assert(row->bucket == bucket->id);
		assert(row->free);
		bucket->list        = row->prev;
		bucket->list_offset = row->prev_offset;
		row->prev           = 0;
		row->prev_offset    = 0;
		row->free           = false;

		// mark bucket as empty
		if (! bucket->list_offset)
			heap_bucket_unset(self, bucket);
	} else
	{
		// ensure current page fits the bucket
		storage_ensure(&self->storage, bucket->size);

		auto page = self->storage.current;
		row = (Row*)page_at(page, page->position);
		row_init(row);
		row->bucket = bucket->id;
		row->offset = page->position;

		page->position += bucket->size;
	}

	// update total used metrics
	self->header->used_count++;
	self->header->used += bucket->size;

	assert(misalign_of(row->data) == 0);
	return row;
}

hot void
heap_remove(Heap* self, Row* row)
{
	assert(! row->free);

	// add row to the free list
	auto bucket = &self->buckets[row->bucket];
	row->free        = true;
	row->prev        = bucket->list;
	row->prev_offset = bucket->list_offset;

	// mark bucket as having free row
	if (! bucket->list_offset)
		heap_bucket_set(self, bucket);

	bucket->list        = heap_page_of(row)->id;
	bucket->list_offset = row->offset;

	// update total used metrics
	self->header->used_count--;
	self->header->used -= bucket->size;
}
