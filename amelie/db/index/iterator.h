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

typedef struct Iterator Iterator;

typedef bool (*IteratorOpen)(Iterator*, Row*);
typedef void (*IteratorNext)(Iterator*);
typedef void (*IteratorClose)(Iterator*);

struct Iterator
{
	Row*          current;
	Timeline*     timeline;
	IteratorNext  next;
	IteratorOpen  open;
	IteratorClose close;
};

always_inline static inline bool
iterator_has(Iterator* self)
{
	return self->current != NULL;
}

always_inline static inline Row*
iterator_at(Iterator* self)
{
	return self->current;
}

always_inline static inline void
iterator_next(Iterator* self)
{
	self->next(self);
	if (! self->timeline)
		return;
	while (self->current)
	{
		if (row_visible(self->current, self->timeline))
			break;
		self->next(self);
	}
}

static inline bool
iterator_open(Iterator* self, Timeline* timeline, Row* key)
{
	self->timeline = timeline;
	auto match = self->open(self, key);
	if (! self->current)
		return false;
	if (! timeline)
		return match;

	if (row_visible(self->current, timeline))
		return match;
	iterator_next(self);
	return false;
}

static inline void
iterator_close(Iterator* self)
{
	if (self->close)
		self->close(self);
}

static inline void
iterator_reset(Iterator* self)
{
	self->current  = NULL;
	self->timeline = NULL;
}

static inline void
iterator_init(Iterator*     self,
              IteratorOpen  open,
              IteratorClose close,
              IteratorNext  next)
{
	self->current  = NULL;
	self->timeline = NULL;
	self->next     = next;
	self->open     = open;
	self->close    = close;
}
