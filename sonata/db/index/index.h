#pragma once

//
// sonata.
//
// SQL Database for JSON.
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
	void      (*ingest)(Index*, Row*);
	Iterator* (*open)(Index*, Row*, bool);
	void      (*free)(Index*);
};

struct Index
{
	IndexIf      iface;
	uint64_t     lsn;
	IndexConfig* config;
	Uuid*        id_storage;
	Uuid*        id_table;
	List         link;
};

static inline void
index_init(Index* self, IndexConfig* config,
           Uuid*  id_table,
           Uuid*  id_storage)
{
	memset(&self->iface, 0, sizeof(*self));
	self->lsn        = 0;
	self->config     = config;
	self->id_table   = id_table;
	self->id_storage = id_storage;
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

static inline void
index_ingest(Index* self, Row* row)
{
	self->iface.ingest(self, row);
}

static inline Iterator*
index_open(Index* self, Row* key, bool start)
{
	return self->iface.open(self, key, start);
}

static inline Def*
index_def(Index* self)
{
	return &self->config->def;
}
