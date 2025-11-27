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
	HeapCursor cursor;
	Heap*      heap;
};

always_inline static inline HeapIterator*
heap_iterator_of(Iterator* self)
{
	return (HeapIterator*)self;
}

static inline bool
heap_iterator_open(Iterator* arg, Row* key)
{
	unused(key);
	auto self = heap_iterator_of(arg);
	return heap_cursor_open(&self->cursor, self->heap);
}

static inline bool
heap_iterator_has(Iterator* arg)
{
	auto self = heap_iterator_of(arg);
	return heap_cursor_has(&self->cursor);
}

static inline Row*
heap_iterator_at(Iterator* arg)
{
	auto self = heap_iterator_of(arg);
	return (Row*)heap_cursor_at(&self->cursor)->data;
}

static inline void
heap_iterator_next(Iterator* arg)
{
	auto self = heap_iterator_of(arg);
	heap_cursor_next(&self->cursor);
}

static inline void
heap_iterator_close(Iterator* arg)
{
	auto self = heap_iterator_of(arg);
	heap_cursor_close(&self->cursor);
	am_free(arg);
}

static inline Iterator*
heap_iterator_allocate(Heap* heap)
{
	HeapIterator* self = am_malloc(sizeof(*self));
	self->it.open  = heap_iterator_open;
	self->it.has   = heap_iterator_has;
	self->it.at    = heap_iterator_at;
	self->it.next  = heap_iterator_next;
	self->it.close = heap_iterator_close;
	self->heap     = heap;
	heap_cursor_init(&self->cursor);
	return &self->it;
}
