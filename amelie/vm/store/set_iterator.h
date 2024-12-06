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

typedef struct SetIterator SetIterator;

struct SetIterator
{
	StoreIterator it;
	Value*        current;
	int           pos;
	Set*          set;
};

always_inline static inline SetIterator*
set_iterator_of(StoreIterator* self)
{
	return (SetIterator*)self;
}

static inline bool
set_iterator_has(StoreIterator* arg)
{
	auto self = set_iterator_of(arg);
	return self->current != NULL;
}

static inline Value*
set_iterator_at(StoreIterator* arg)
{
	auto self = set_iterator_of(arg);
	return self->current;
}

static inline void
set_iterator_next(StoreIterator* arg)
{
	auto self = set_iterator_of(arg);
	if (self->current == NULL)
		return;
	self->pos++;
	if (likely(self->pos < self->set->count_rows))
		self->current = set_row_of(self->set, self->pos);
	else
		self->current = NULL;
}

static inline void
set_iterator_close(StoreIterator* arg)
{
	am_free(arg);
}

static inline StoreIterator*
set_iterator_allocate(Set* set)
{
	SetIterator* self = am_malloc(sizeof(*self));
	self->it.has   = set_iterator_has;
	self->it.at    = set_iterator_at;
	self->it.next  = set_iterator_next;
	self->it.close = set_iterator_close;
	self->pos      = 0;
	self->set      = set;
	if (set->count > 0)
		self->current = set_row_of(set, 0);
	else
		self->current = NULL;
	return &self->it;
}
