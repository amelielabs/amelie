#pragma once

//
// indigo
//
// SQL OLTP database
//

typedef struct Iterator Iterator;

typedef bool (*IteratorHas)(Iterator*);
typedef Row* (*IteratorAt)(Iterator*);
typedef void (*IteratorNext)(Iterator*);
typedef void (*IteratorClose)(Iterator*);

struct Iterator
{
	IteratorHas   has;
	IteratorAt    at;
	IteratorNext  next;
	IteratorClose close;
};

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
