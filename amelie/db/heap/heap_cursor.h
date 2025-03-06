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

typedef struct HeapCursor HeapCursor;

struct HeapCursor
{
	Chunk*   current;
	Page*    page;
	int      page_order;
	PageMgr* page_mgr;
	Heap*    heap;
};

hot static inline void
heap_cursor_next_chunk(HeapCursor* self)
{
	if (unlikely(! self->current))
		return;
	if (likely(! self->current->last))
	{
		auto next = (uintptr_t)self->current + self->current->size;
		self->current = (Chunk*)next;
		return;
	}
	self->current = NULL;
	self->page_order++;
	if (unlikely(self->page_order >= self->page_mgr->list_count))
		return;
	self->page = page_mgr_at(self->page_mgr, self->page_order);
	self->current = (Chunk*)(self->page->pointer + sizeof(PageHeader));
}

hot static inline void
heap_cursor_next_allocated(HeapCursor* self)
{
	while (self->current && self->current->free)
		heap_cursor_next_chunk(self);
}

static inline bool
heap_cursor_open(HeapCursor* self, Heap* heap)
{
	if (unlikely(! heap->last))
		return false;
	self->heap       = heap;
	self->page_mgr   = &heap->page_mgr;
	self->page       = page_mgr_at(self->page_mgr, 0);
	self->page_order = 0;
	self->current    = heap_first(heap);
	heap_cursor_next_allocated(self);
	return self->current != NULL;
}

static inline void
heap_cursor_close(HeapCursor* self)
{
	unused(self);
}

always_inline static inline bool
heap_cursor_has(HeapCursor* self)
{
	return self->current != NULL;
}

always_inline static inline Chunk*
heap_cursor_at(HeapCursor* self)
{
	return self->current;
}

static inline void
heap_cursor_init(HeapCursor* self)
{
	self->current    = NULL;
	self->page       = NULL;
	self->page_order = 0;
	self->page_mgr   = NULL;
	self->heap       = NULL;
}

always_inline static inline void
heap_cursor_next(HeapCursor* self)
{
	heap_cursor_next_chunk(self);
	heap_cursor_next_allocated(self);
}
