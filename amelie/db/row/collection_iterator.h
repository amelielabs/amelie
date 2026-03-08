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

typedef struct CollectionIterator CollectionIterator;

struct CollectionIterator
{
	Iterator    it;
	Row**       current;
	Row**       end;
	Collection* col;
};

static inline bool
collection_iterator_open(CollectionIterator* self, Collection* col)
{
	if (unlikely(collection_empty(col)))
		return false;
	auto buf = &col->buf;
	self->current = (Row**)buf->start;
	self->end     = (Row**)buf->position;
	self->col     = col;
	return true;
}

static inline bool
collection_iterator_has(CollectionIterator* self)
{
	return self->current != NULL;
}

static inline Row*
collection_iterator_at(CollectionIterator* self)
{
	return *self->current;
}

static inline void
collection_iterator_next(CollectionIterator* self)
{
	if (unlikely(! self->current))
		return;
	self->current++;
	if (unlikely(self->current >= self->end))
		self->current = NULL;
}

static inline void
collection_iterator_init(CollectionIterator* self)
{
	self->current = NULL;
	self->end     = NULL;
	self->col     = NULL;
	auto it = &self->it;
	it->has   = (IteratorHas)collection_iterator_has;
	it->at    = (IteratorAt)collection_iterator_at;
	it->next  = (IteratorNext)collection_iterator_next;
	it->close = NULL;
}

static inline void
collection_iterator_reset(CollectionIterator* self)
{
	self->current = NULL;
	self->end     = NULL;
	self->col     = NULL;
}
