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
	Branch*       branch;
	Heap*         heap;
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
	if (! self->heap)
		return;
	while (self->current)
	{
		auto visible = row_visible(self->current, self->heap, self->branch);
		if (visible)
		{
			self->current = visible;
			break;
		}
		self->next(self);
	}
}

static inline bool
iterator_open(Iterator* self, Heap* heap, Branch* branch, Row* key)
{
	self->branch = branch;
	self->heap   = heap;

	auto match = self->open(self, key);
	if (! self->current)
		return false;
	if (! heap)
		return match;

	// set visible version
	auto visible = row_visible(self->current, heap, branch);
	if (visible)
	{
		self->current = visible;
		return match;
	}
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
	self->current = NULL;
	self->branch  = NULL;
	self->heap    = NULL;
}

static inline void
iterator_init(Iterator*     self,
              IteratorOpen  open,
              IteratorClose close,
              IteratorNext  next)
{
	self->current = NULL;
	self->branch  = NULL;
	self->heap    = NULL;
	self->next    = next;
	self->open    = open;
	self->close   = close;
}
