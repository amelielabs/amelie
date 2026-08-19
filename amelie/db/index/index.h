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

typedef struct IndexIf IndexIf;
typedef struct IndexOp IndexOp;
typedef struct Index   Index;

struct IndexIf
{
	bool      (*replace)(Index*, IndexOp*);
	bool      (*upsert)(Index*, IndexOp*);
	bool      (*delete)(Index*, IndexOp*);
	void      (*truncate)(Index*, IndexOp*);
	void      (*create)(Index*, IndexOp*);
	void      (*free)(Index*, IndexOp*);
	Iterator* (*iterator)(Index*);
	Iterator* (*iterator_merge)(Index*, Iterator*, Heap*);
};

struct IndexOp
{
	Row*      row;
	Row*      row_prev;
	Iterator* it;
	int64_t   delta;
};

struct Index
{
	IndexIf      iface;
	void*        iface_arg;
	IndexConfig* config;
	Index*       next;
};

static inline void
index_op_init(IndexOp* self)
{
	memset(self, 0, sizeof(*self));
}

static inline void
index_op_set(IndexOp* self, Row* row)
{
	self->row      = row;
	self->row_prev = NULL;
	self->it       = NULL;
	self->delta    = 0;
}

static inline void
index_init(Index* self, IndexConfig* config, void* arg)
{
	memset(self, 0, sizeof(*self));
	self->config    = config;
	self->iface_arg = arg;
	self->next      = NULL;
}

static inline void
index_create(Index* self, IndexOp* op)
{
	self->iface.create(self, op);
}

static inline void
index_free(Index* self, IndexOp* op)
{
	self->iface.free(self, op);
}

static inline void
index_truncate(Index* self, IndexOp* op)
{
	self->iface.truncate(self, op);
}

static inline bool
index_upsert(Index* self, IndexOp* op)
{
	return self->iface.upsert(self, op);
}

static inline bool
index_replace(Index* self, IndexOp* op)
{
	return self->iface.replace(self, op);
}

static inline bool
index_delete(Index* self, IndexOp* op)
{
	return self->iface.delete(self, op);
}

static inline Iterator*
index_iterator(Index* self)
{
	return self->iface.iterator(self);
}

static inline Iterator*
index_iterator_merge(Index* self, Iterator* it, Heap* heap)
{
	return self->iface.iterator_merge(self, it, heap);
}

static inline Keys*
index_keys(Index* self)
{
	return &self->config->keys;
}
