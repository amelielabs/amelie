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
	Row*   current;
	Page*  page;
	int    page_order;
	Pages* pages;
	bool   eof;
	Row*   stop;
	Heap*  heap;
};

hot static inline void
heap_iterator_next_row(HeapIterator* self)
{
	auto current = self->current;
	if (unlikely(! current))
	{
		// first iterator after empty heap open
		auto pages = &self->heap->pages;
		if (pages_empty(pages))
			return;

		// first
		self->page       = pages_at(pages, 0);
		self->page_order = 0;
		self->eof        = false;
		self->current    = heap_at(self->heap, 0, sizeof(Page));
		return;
	}

	// reached last row to stop
	if (unlikely(current == self->stop))
	{
		self->eof = true;
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
	if (unlikely((self->page_order + 1) >= self->pages->count))
	{
		self->eof = true;
		return;
	}

	self->eof = false;
	self->page_order++;
	self->page = pages_at(self->pages, self->page_order);
	self->current = (Row*)page_at(self->page, sizeof(Page));
}

hot static inline void
heap_iterator_next_allocated(HeapIterator* self)
{
	while (!self->eof && self->current)
	{
		if (!self->current->free && !self->current->deleted)
			break;
		heap_iterator_next_row(self);
	}
}

static inline void
heap_iterator_open(HeapIterator* self, Heap* heap, bool last)
{
	auto pages = &heap->pages;
	self->pages = pages;
	self->heap  = heap;
	if (pages_empty(pages))
	{
		self->page       = NULL;
		self->page_order = 0;
		self->eof        = true;
		self->current    = NULL;
		return;
	}

	if (last)
	{
		auto order = pages->count - 1;
		self->page       = pages_at(pages, order);
		self->page_order = order;
		self->eof        = true;
		self->current    = heap_at(heap, order, self->page->position_last);
		return;
	}

	// first
	self->page       = pages_at(pages, 0);
	self->page_order = 0;
	self->eof        = false;
	self->current    = heap_at(heap, 0, sizeof(Page));
	heap_iterator_next_allocated(self);
}

static inline bool
heap_iterator_set(HeapIterator* self, uint32_t page_id, uint32_t page_offset)
{
	auto heap  = self->heap;
	auto pages = &heap->pages;
	if (pages_empty(pages))
		return false;

	// validate position
	if (page_id < pages->id_first)
		return false;

	auto last = pages_at(pages, pages->count - 1);
	if (page_id > last->id)
		return false;

	auto page = pages_get(pages, page_id);
	if (page_offset > page->position_last)
		return false;
	if (page_offset < sizeof(Page))
		return false;

	auto row = heap_at(heap, page_id, page_offset);
	if (row->offset != page_offset || row->free)
		return false;
	if (heap_page_of(row)->id != page_id)
		return false;

	self->page       = page;
	self->page_order = page->id - pages->id_first;
	self->eof        = false;
	self->current    = row;
	return true;
}

static inline void
heap_iterator_set_stop(HeapIterator* self)
{
	auto pages = self->pages;
	if (pages_empty(pages))
		return;
	auto page_order = pages->count - 1;
	auto page = pages_at(pages, page_order);
	self->stop = heap_at(self->heap, page_order, page->position_last);
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
	self->stop       = NULL;
	self->pages      = NULL;
	self->heap       = NULL;
}

static inline void
heap_iterator_reset(HeapIterator* self)
{
	self->current    = NULL;
	self->page       = NULL;
	self->page_order = 0;
	self->eof        = false;
	self->stop       = NULL;
	self->pages      = NULL;
	self->heap       = NULL;
}
