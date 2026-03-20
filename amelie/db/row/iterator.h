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
	IteratorNext  next;
	IteratorOpen  open;
	IteratorClose close;
};

static inline void
iterator_init(Iterator*     self,
              IteratorOpen  open,
              IteratorClose close,
              IteratorNext  next)
{
	self->current = NULL;
	self->next    = next;
	self->open    = open;
	self->close   = close;
}

static inline bool
iterator_open(Iterator* self, Row* key)
{
	return self->open(self, key);
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
	self->current = NULL;
}

static inline void
iterator_next(Iterator* self)
{
	self->next(self);
}

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
