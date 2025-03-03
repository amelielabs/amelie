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
typedef struct Index   Index;

struct IndexIf
{
	Row*      (*set)(Index*, Row*);
	Row*      (*update)(Index*, Row*, Iterator*);
	Row*      (*delete)(Index*, Iterator*);
	Row*      (*delete_by)(Index*, Row*);
	bool      (*upsert)(Index*, Row*, Iterator*);
	bool      (*ingest)(Index*, Row*);
	Iterator* (*iterator)(Index*);
	void      (*truncate)(Index*);
	void      (*free)(Index*);
};

struct Index
{
	IndexIf      iface;
	IndexConfig* config;
	Heap*        heap;
	List         link;
};

static inline void
index_init(Index* self, IndexConfig* config, Heap* heap)
{
	memset(&self->iface, 0, sizeof(*self));
	self->config = config;
	self->heap   = heap;
	list_init(&self->link);
}

static inline void
index_free(Index* self)
{
	self->iface.free(self);
}

static inline void
index_truncate(Index* self)
{
	self->iface.truncate(self);
}

static inline Row*
index_set(Index* self, Row* key)
{
	return self->iface.set(self, key);
}

static inline Row*
index_update(Index* self, Row* key, Iterator* it)
{
	return self->iface.update(self, key, it);
}

static inline Row*
index_delete(Index* self, Iterator* it)
{
	return self->iface.delete(self, it);
}

static inline Row*
index_delete_by(Index* self, Row* key)
{
	return self->iface.delete_by(self, key);
}

static inline bool
index_upsert(Index* self, Row* key, Iterator* it)
{
	return self->iface.upsert(self, key, it);
}

static inline bool
index_ingest(Index* self, Row* key)
{
	return self->iface.ingest(self, key);
}

static inline Iterator*
index_iterator(Index* self)
{
	return self->iface.iterator(self);
}

static inline Keys*
index_keys(Index* self)
{
	return &self->config->keys;
}
