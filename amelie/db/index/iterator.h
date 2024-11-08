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
typedef bool (*IteratorHas)(Iterator*);
typedef Row* (*IteratorAt)(Iterator*);
typedef void (*IteratorNext)(Iterator*);
typedef void (*IteratorClose)(Iterator*);

struct Iterator
{
	IteratorOpen  open;
	IteratorHas   has;
	IteratorAt    at;
	IteratorNext  next;
	IteratorClose close;
};

static inline bool
iterator_open(Iterator* self, Row* key)
{
	return self->open(self, key);
}

static inline bool
iterator_has(Iterator* self)
{
	return self->has(self);
}

static inline Row*
iterator_at(Iterator* self)
{
	return self->at(self);
}

static inline void
iterator_next(Iterator* self)
{
	self->next(self);
}

static inline void
iterator_close(Iterator* self)
{
	if (self->close)
		self->close(self);
}
