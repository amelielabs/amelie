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
	bool     eof;
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
		self->eof = false;
		return;
	}

	// next page
	if (unlikely((self->page_order + 1) >= self->storage->list_count))
	{
		self->eof = true;
		return;
	}

	self->eof = false;
	self->page_order++;
	self->page = storage_at(self->storage, self->page_order);
	self->current = (Row*)page_at(self->page, sizeof(Page));
}

hot static inline void
heap_iterator_next_allocated(HeapIterator* self)
{
	while (!self->eof && self->current && self->current->free)
		heap_iterator_next_row(self);
}

static inline bool
heap_iterator_open(HeapIterator* self, Heap* heap)
{
	if (unlikely(! heap->header->used_count))
		return false;
	self->heap       = heap;
	self->storage    = &heap->storage;
	self->page       = storage_at(self->storage, 0);
	self->page_order = 0;
	self->eof        = false;
	self->current    = heap_at(heap, 0, sizeof(Page));
	heap_iterator_next_allocated(self);
	return self->current != NULL;
}

always_inline static inline bool
heap_iterator_has(HeapIterator* self)
{
	if (unlikely(self->eof))
		return false;
	return self->current != NULL;
}

always_inline static inline bool
heap_iterator_active(HeapIterator* self)
{
	return self->heap != NULL;
}

always_inline static inline Row*
heap_iterator_at(HeapIterator* self)
{
	if (unlikely(self->eof))
		return NULL;
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
	self->eof        = false;
	self->storage    = NULL;
	self->heap       = NULL;
}

static inline void
heap_iterator_reset(HeapIterator* self)
{
	self->current    = NULL;
	self->page       = NULL;
	self->page_order = 0;
	self->eof        = false;
	self->storage    = NULL;
	self->heap       = NULL;
}
