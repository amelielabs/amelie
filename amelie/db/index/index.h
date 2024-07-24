#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct IndexIf IndexIf;
typedef struct Index   Index;

struct IndexIf
{
	bool      (*set)(Index*, Ref*, Ref*);
	void      (*update)(Index*, Ref*, Ref*, Iterator*);
	void      (*delete)(Index*, Ref*, Ref*, Iterator*);
	bool      (*delete_by)(Index*, Ref*, Ref*);
	bool      (*upsert)(Index*, Ref*, Iterator*);
	bool      (*ingest)(Index*, Ref*);
	Iterator* (*iterator)(Index*);
	void      (*truncate)(Index*);
	void      (*free)(Index*);
};

struct Index
{
	IndexIf      iface;
	IndexConfig* config;
	List         link;
};

static inline void
index_init(Index* self, IndexConfig* config)
{
	memset(&self->iface, 0, sizeof(*self));
	self->config = config;
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

static inline bool
index_set(Index* self, Ref* key, Ref* prev)
{
	return self->iface.set(self, key, prev);
}

static inline void
index_update(Index* self, Ref* key, Ref* prev, Iterator* it)
{
	self->iface.update(self, key, prev, it);
}

static inline void
index_delete(Index* self, Ref* key, Ref* prev, Iterator* it)
{
	self->iface.delete(self, key, prev, it);
}

static inline bool
index_delete_by(Index* self, Ref* key, Ref* prev)
{
	return self->iface.delete_by(self, key, prev);
}

static inline bool
index_upsert(Index* self, Ref* key, Iterator* it)
{
	return self->iface.upsert(self, key, it);
}

static inline bool
index_ingest(Index* self, Ref* key)
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
