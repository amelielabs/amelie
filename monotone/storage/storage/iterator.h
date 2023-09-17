#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Iterator Iterator;

struct Iterator
{
	void (*open)(Iterator*, Row*);
	bool (*has)(Iterator*);
	Row* (*at)(Iterator*);
	void (*next)(Iterator*);
	void (*close)(Iterator*);
	void (*free)(Iterator*);
};

static inline void
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
	self->close(self);
}

static inline void
iterator_free(Iterator* self)
{
	self->free(self);
}
