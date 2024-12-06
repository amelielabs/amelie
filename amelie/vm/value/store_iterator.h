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

typedef struct StoreIterator StoreIterator;
typedef struct Value         Value;

struct StoreIterator
{
	bool   (*has)(StoreIterator*);
	Value* (*at)(StoreIterator*);
	void   (*next)(StoreIterator*);
	void   (*close)(StoreIterator*);
};

always_inline static inline bool
store_iterator_has(StoreIterator* self)
{
	return self->has(self);
}

always_inline static inline Value*
store_iterator_at(StoreIterator* self)
{
	return self->at(self);
}

always_inline static inline void
store_iterator_next(StoreIterator* self)
{
	self->next(self);
}

always_inline static inline void
store_iterator_close(StoreIterator* self)
{
	self->close(self);
}
