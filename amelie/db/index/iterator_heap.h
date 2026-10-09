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

typedef struct IteratorHeap IteratorHeap;

struct IteratorHeap
{
	Iterator     it;
	HeapIterator iterator;
	Heap*        heap;
};

always_inline static inline IteratorHeap*
iterator_heap_of(Iterator* self)
{
	return (IteratorHeap*)self;
}

static inline bool
iterator_heap_open(Iterator* arg, Row* key)
{
	unused(key);
	auto self = iterator_heap_of(arg);
	heap_iterator_open(&self->iterator, self->heap, false);
	arg->current = heap_iterator_at(&self->iterator);
	return false;
}

static inline void
iterator_heap_next(Iterator* arg)
{
	auto self = iterator_heap_of(arg);
	heap_iterator_next(&self->iterator);
	arg->current = heap_iterator_at(&self->iterator);
}

static inline void
iterator_heap_close(Iterator* arg)
{
	am_free(arg);
}

static inline void
iterator_heap_init(IteratorHeap* self, Heap* heap)
{
	self->heap = heap;
	heap_iterator_init(&self->iterator);
	iterator_init(&self->it,
	              iterator_heap_open,
	              iterator_heap_close,
	              iterator_heap_next);
}

static inline Iterator*
iterator_heap_allocate(Heap* heap)
{
	IteratorHeap* self = am_malloc(sizeof(*self));
	iterator_heap_init(self, heap);
	return &self->it;
}
