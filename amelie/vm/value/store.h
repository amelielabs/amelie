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
	int             columns_row;
	int             columns;
	int             keys;
	bool*           keys_order;
	StoreIterator* (*iterator)(Store*);
	void           (*free)(Store*);
};

static inline void
store_init(Store* self, int type)
{
	self->refs        = 0;
	self->type        = type;
	self->columns_row = 0;
	self->columns     = 0;
	self->keys        = 0;
	self->keys_order  = NULL;
	self->iterator    = NULL;
	self->free        = NULL;
}

static inline void
store_set(Store* self,
          int    columns,
          int    keys,
          bool*  keys_order)
{
	self->columns_row = columns + keys;
	self->columns     = columns;
	self->keys        = keys;
	self->keys_order  = keys_order;
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

static inline bool
store_ordered(Store* self)
{
	return self->keys_order != NULL;
}
