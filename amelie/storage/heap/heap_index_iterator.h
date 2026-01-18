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

typedef struct HeapIndexIterator HeapIndexIterator;

struct HeapIndexIterator
{
	Iterator it;
	Row**    current;
	Row**    end;
	Buf*     index;
};

static inline bool
heap_index_iterator_open(HeapIndexIterator* self, Buf* index, Row* key)
{
	unused(key);
	if (unlikely(buf_empty(index)))
		return false;
	self->current = (Row**)index->start;
	self->end     = (Row**)index->position;
	self->index   = index;
	return true;
}

static inline bool
heap_index_iterator_has(HeapIndexIterator* self)
{
	return self->current != NULL;
}

static inline Row*
heap_index_iterator_at(HeapIndexIterator* self)
{
	return *self->current;
}

static inline void
heap_index_iterator_next(HeapIndexIterator* self)
{
	if (unlikely(! self->current))
		return;
	if (likely(self->current < self->end))
		self->current++;
	else
		self->current = NULL;
}

static inline void
heap_index_iterator_init(HeapIndexIterator* self)
{
	self->current = NULL;
	self->end     = NULL;
	self->index   = NULL;
	auto it = &self->it;
	it->has   = (IteratorHas)heap_index_iterator_has;
	it->at    = (IteratorAt)heap_index_iterator_at;
	it->next  = (IteratorNext)heap_index_iterator_next;
	it->close = NULL;
}

static inline void
heap_index_iterator_reset(HeapIndexIterator* self)
{
	self->current = NULL;
	self->end     = NULL;
	self->index   = NULL;
}
