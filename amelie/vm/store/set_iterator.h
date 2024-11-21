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
	Value* current;
	int    pos;
	Set*   set;
};

static inline void
set_iterator_init(SetIterator* self)
{
	self->current = 0;
	self->pos     = 0;
	self->set     = NULL;
}

static inline void
set_iterator_open(SetIterator* self, Set* set)
{
	self->current = NULL;
	self->pos     = 0;
	self->set     = set;
	if (set->count > 0)
		self->current = set_row_of(set, 0);
}

static inline bool
set_iterator_has(SetIterator* self)
{
	return self->current != NULL;
}

static inline Value*
set_iterator_at(SetIterator* self)
{
	return self->current;
}

static inline void
set_iterator_next(SetIterator* self)
{
	if (self->current == NULL)
		return;
	self->pos++;
	if (likely(self->pos < self->set->count_rows))
		self->current = set_row_of(self->set, self->pos);
	else
		self->current = NULL;
}
