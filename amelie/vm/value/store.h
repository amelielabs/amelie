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

typedef struct Store Store;

enum
{
	STORE_SET,
	STORE_UNION
};

struct Store
{
	atomic_u32      refs;
	int             type;
	StoreIterator* (*iterator)(Store*);
	void           (*free)(Store*);
};

static inline void
store_init(Store* self, int type)
{
	self->refs     = 0;
	self->type     = type;
	self->iterator = NULL;
	self->free     = NULL;
}

static inline void
store_free(Store* self)
{
	if (atomic_u32_dec(&self->refs) == 0)
		self->free(self);
}

static inline void
store_ref(Store* self)
{
	atomic_u32_inc(&self->refs);
}

static inline StoreIterator*
store_iterator(Store* self)
{
	return self->iterator(self);
}
