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

typedef struct HeapIterator HeapIterator;

struct HeapIterator
{
	Row*     current;
	Page*    page;
	int      page_order;
	Storage* storage;
	Heap*    heap;
};

hot static inline void
heap_iterator_next_row(HeapIterator* self)
{
	auto current = self->current;
	if (unlikely(! current))
		return;

	// next row
	auto next = (uintptr_t)current + self->heap->buckets[current->bucket].size;
	auto end  = page_end(self->page);
	if (likely(next < end))
	{
		self->current = (Row*)next;
		return;
	}

	// next page
	self->current = NULL;
	self->page_order++;
	if (unlikely(self->page_order >= self->storage->list_count))
		return;
	self->page = storage_at(self->storage, self->page_order);
	self->current = (Row*)page_at(self->page, sizeof(Page));
}

hot static inline void
heap_iterator_next_allocated(HeapIterator* self)
{
	while (self->current && self->current->free)
		heap_iterator_next_row(self);
}

static inline bool
heap_iterator_open(HeapIterator* self, Heap* heap, Row* key)
{
	unused(key);
	if (unlikely(! heap->header->used_count))
		return false;
	self->heap       = heap;
	self->storage    = &heap->storage;
	self->page       = storage_at(self->storage, 0);
	self->page_order = 0;
	self->current    = heap_at(heap, 0, sizeof(Page));
	heap_iterator_next_allocated(self);
	return self->current != NULL;
}

always_inline static inline bool
heap_iterator_has(HeapIterator* self)
{
	return self->current != NULL;
}

always_inline static inline Row*
heap_iterator_at(HeapIterator* self)
{
	return self->current;
}

static inline void
heap_iterator_next(HeapIterator* self)
{
	heap_iterator_next_row(self);
	heap_iterator_next_allocated(self);
}

static inline void
heap_iterator_init(HeapIterator* self)
{
	self->current    = NULL;
	self->page       = NULL;
	self->page_order = 0;
	self->storage    = NULL;
	self->heap       = NULL;
}

static inline void
heap_iterator_reset(HeapIterator* self)
{
	self->current    = NULL;
	self->page       = NULL;
	self->page_order = 0;
	self->storage    = NULL;
	self->heap       = NULL;
}
