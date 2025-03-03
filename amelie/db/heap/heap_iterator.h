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
	Chunk*   current;
	Page*    page;
	int      page_order;
	PageMgr* page_mgr;
	Heap*    heap;
};

hot static inline void
heap_iterator_next_chunk(HeapIterator* self)
{
	if (unlikely(! self->current))
		return;
	auto next = (uintptr_t)self->current + self->current->size;
	auto page_end = (uintptr_t)self->page->pointer + self->page->pos;
	if (likely(next < page_end))
	{
		self->current = (Chunk*)next;
		return;
	}
	self->current = NULL;
	self->page_order++;
	if (unlikely(self->page_order >= self->page_mgr->list_count))
		return;
	self->page = page_mgr_at(self->page_mgr, self->page_order);
	if (self->page->pos > 0)
		self->current = (Chunk*)self->page->pointer;
}

hot static inline void
heap_iterator_next_allocated(HeapIterator* self)
{
	while (self->current && self->current->is_free)
		heap_iterator_next_chunk(self);
}

static inline void
heap_iterator_open(HeapIterator* self, Heap* heap)
{
	if (unlikely(! heap->page_mgr.list_count))
		return;
	self->page       = page_mgr_at(self->page_mgr, 0);
	self->page_order = 0;
	self->page_mgr   = &heap->page_mgr;
	self->heap       = heap;
	if (self->page->pos > 0)
		self->current = (Chunk*)self->page->pointer;
	heap_iterator_next_allocated(self);
}

static inline void
heap_iterator_close(HeapIterator* self)
{
	unused(self);
}

always_inline static inline bool
heap_iterator_has(HeapIterator* self)
{
	return self->current != NULL;
}

always_inline static inline Chunk*
heap_iterator_at(HeapIterator* self)
{
	return self->current;
}

static inline void
heap_iterator_init(HeapIterator* self)
{
	self->current    = NULL;
	self->page       = NULL;
	self->page_order = 0;
	self->page_mgr   = NULL;
	self->heap       = NULL;
}

always_inline static inline void
heap_iterator_next(HeapIterator* self)
{
	heap_iterator_next_chunk(self);
	heap_iterator_next_allocated(self);
}
