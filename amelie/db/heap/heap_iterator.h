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
	{
		// first iterator after empty heap open
		auto storage = &self->heap->storage;
		if (storage_empty(storage))
			return;

		// first
		self->page       = storage_at(storage, 0);
		self->page_order = 0;
		self->eof        = false;
		self->current    = heap_at(self->heap, 0, sizeof(Page));
		return;
	}

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

static inline void
heap_iterator_open(HeapIterator* self, Heap* heap, bool last)
{
	auto storage = &heap->storage;
	self->storage = storage;
	self->heap    = heap;
	if (storage_empty(storage))
	{
		self->page       = NULL;
		self->page_order = 0;
		self->eof        = true;
		self->current    = NULL;
		return;
	}

	if (last)
	{
		auto order = storage->list_count - 1;
		self->page       = storage_at(storage, order);
		self->page_order = order;
		self->eof        = true;
		self->current    = heap_at(heap, 0, self->page->position_last);
		return;
	}

	// first
	self->page       = storage_at(storage, 0);
	self->page_order = 0;
	self->eof        = false;
	self->current    = heap_at(heap, 0, sizeof(Page));
	heap_iterator_next_allocated(self);
}

static inline void
heap_iterator_set(HeapIterator* self, Row* row)
{
	self->current    = row;
	self->page       = heap_page_of(row);
	self->page_order = self->page->id - self->heap->storage.id_first;
	self->eof        = false;
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
