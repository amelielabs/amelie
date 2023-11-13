#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct SetIterator SetIterator;

struct SetIterator
{
	SetRow* current;
	int     pos;
	bool    advance;
	Set*    set;
};

static inline void
set_iterator_init(SetIterator* self)
{
	self->current = NULL;
	self->pos     = 0;
	self->advance = false;
	self->set     = 0;
}

static inline void
set_iterator_open(SetIterator* self, Set* set)
{
	self->pos = 0;
	self->set = set;
	if (set->list_count > 0)
		self->current = set_at(set, 0);
}

static inline bool
set_iterator_has(SetIterator* self)
{
	return self->current != NULL;
}

static inline SetRow*
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
	if (unlikely(self->pos >= self->set->list_count))
		self->current = NULL;
	else
		self->current = set_at(self->set, self->pos);
}
