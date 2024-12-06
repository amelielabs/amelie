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
	int           pos;
	Set*          set;
};

always_inline static inline SetIterator*
set_iterator_of(StoreIterator* self)
{
	return (SetIterator*)self;
}

static inline void
set_iterator_next(StoreIterator* arg)
{
	if (arg->current == NULL)
		return;
	auto self = set_iterator_of(arg);
	self->pos++;
	if (likely(self->pos < self->set->count_rows))
		arg->current = set_row_of(self->set, self->pos);
	else
		arg->current = NULL;
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
	self->it.next    = set_iterator_next;
	self->it.close   = set_iterator_close;
	self->it.current = NULL;
	self->pos        = 0;
	self->set        = set;
	if (set->count > 0)
		self->it.current = set_row_of(set, 0);
	return &self->it;
}
