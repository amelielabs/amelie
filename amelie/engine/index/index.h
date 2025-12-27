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
	bool      (*upsert)(Index*, Row*, Iterator*);
	Row*      (*replace_by)(Index*, Row*);
	Row*      (*replace)(Index*, Row*, Iterator*);
	Row*      (*delete_by)(Index*, Row*);
	Row*      (*delete)(Index*, Iterator*);
	Iterator* (*iterator)(Index*);
	Iterator* (*iterator_merge)(Index*, Iterator*);
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

static inline bool
index_upsert(Index* self, Row* key, Iterator* it)
{
	return self->iface.upsert(self, key, it);
}

static inline Row*
index_replace_by(Index* self, Row* key)
{
	return self->iface.replace_by(self, key);
}

static inline Row*
index_replace(Index* self, Row* key, Iterator* it)
{
	return self->iface.replace(self, key, it);
}

static inline Row*
index_delete_by(Index* self, Row* key)
{
	return self->iface.delete_by(self, key);
}

static inline Row*
index_delete(Index* self, Iterator* it)
{
	return self->iface.delete(self, it);
}

static inline Iterator*
index_iterator(Index* self)
{
	return self->iface.iterator(self);
}

static inline Iterator*
index_iterator_merge(Index* self, Iterator* it)
{
	return self->iface.iterator_merge(self, it);
}

static inline void
index_truncate(Index* self)
{
	self->iface.truncate(self);
}

static inline Keys*
index_keys(Index* self)
{
	return &self->config->keys;
}
