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
	Iterator   it;
	HeapChunk* current;
	Page*      page;
	int        page_order;
	PageMgr*   page_mgr;
	Heap*      heap;
};

hot static inline void
heap_iterator_next_chunk(HeapIterator* self)
{
	if (unlikely(! self->current))
		return;
	if (likely(! self->current->is_last))
	{
		auto next = (uintptr_t)self->current + self->heap->buckets[self->current->bucket].size;
		self->current = (HeapChunk*)next;
		return;
	}
	self->current = NULL;
	self->page_order++;
	if (unlikely(self->page_order >= self->page_mgr->list_count))
		return;
	self->page = page_mgr_at(self->page_mgr, self->page_order);
	self->current = (HeapChunk*)(self->page->pointer + sizeof(PageHeader));
}

hot static inline void
heap_iterator_next_allocated(HeapIterator* self)
{
	while (self->current && self->current->is_free)
		heap_iterator_next_chunk(self);
}

static inline bool
heap_iterator_open(HeapIterator* self, Heap* heap, Row* key)
{
	unused(key);
	if (unlikely(! heap->last))
		return false;
	self->heap       = heap;
	self->page_mgr   = &heap->page_mgr;
	self->page       = page_mgr_at(self->page_mgr, 0);
	self->page_order = 0;
	self->current    = heap_first(heap);
	heap_iterator_next_allocated(self);
	return self->current != NULL;
}

static inline bool
heap_iterator_has(HeapIterator* self)
{
	return self->current != NULL;
}

static inline Row*
heap_iterator_at(HeapIterator* self)
{
	return (Row*)self->current->data;
}

static inline void
heap_iterator_next(HeapIterator* self)
{
	heap_iterator_next_chunk(self);
	heap_iterator_next_allocated(self);
}

static inline void
heap_iterator_init(HeapIterator* self)
{
	self->current    = NULL;
	self->page       = NULL;
	self->page_order = 0;
	self->page_mgr   = NULL;
	self->heap       = NULL;
	auto it = &self->it;
	it->has   = (IteratorHas)heap_iterator_has;
	it->at    = (IteratorAt)heap_iterator_at;
	it->next  = (IteratorNext)heap_iterator_next;
	it->close = NULL;
}

static inline void
heap_iterator_reset(HeapIterator* self)
{
	self->current    = NULL;
	self->page       = NULL;
	self->page_order = 0;
	self->page_mgr   = NULL;
	self->heap       = NULL;
}
