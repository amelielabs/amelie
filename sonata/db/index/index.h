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
	bool      (*set)(Index*, Transaction*, Row*);
	void      (*update)(Index*, Transaction*, Iterator*, Row*);
	void      (*delete)(Index*, Transaction*, Iterator*);
	void      (*delete_by)(Index*, Transaction*, Row*);
	void      (*upsert)(Index*, Transaction*, Iterator**, Row*);
	bool      (*ingest)(Index*, Row*);
	Iterator* (*open)(Index*, Ref*, bool);
	void      (*free)(Index*);
};

struct Index
{
	IndexIf      iface;
	uint64_t     partition;
	IndexConfig* config;
	List         link;
};

static inline void
index_init(Index* self, IndexConfig* config, uint64_t partition)
{
	memset(&self->iface, 0, sizeof(*self));
	self->partition = partition;
	self->config    = config;
	list_init(&self->link);
}

static inline void
index_free(Index* self)
{
	self->iface.free(self);
}

static inline bool
index_set(Index* self, Transaction* trx, Row* row)
{
	return self->iface.set(self, trx, row);
}

static inline void
index_update(Index* self, Transaction* trx, Iterator* it, Row* row)
{
	self->iface.update(self, trx, it, row);
}

static inline void
index_delete(Index* self, Transaction* trx, Iterator* it)
{
	self->iface.delete(self, trx, it);
}

static inline void
index_delete_by(Index* self, Transaction* trx, Row* key)
{
	self->iface.delete_by(self, trx, key);
}

static inline void
index_upsert(Index* self, Transaction* trx, Iterator** it, Row* row)
{
	self->iface.upsert(self, trx, it, row);
}

static inline bool
index_ingest(Index* self, Row* row)
{
	return self->iface.ingest(self, row);
}

static inline Iterator*
index_open(Index* self, Ref* key, bool start)
{
	return self->iface.open(self, key, start);
}

static inline Iterator*
index_open_by(Index* self, Row* row, bool start)
{
	auto    keys = &self->config->keys;
	uint8_t key_data[keys->key_size];
	auto    key = (Ref*)key_data;
	ref_create(key, row, keys);
	return self->iface.open(self, key, start);
}

static inline Keys*
index_keys(Index* self)
{
	return &self->config->keys;
}
