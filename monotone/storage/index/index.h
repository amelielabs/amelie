#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Index Index;

struct Index
{
	void        (*free)(Index*);
	bool        (*set)(Index*, Transaction*, Row*);
	void        (*update)(Index*, Transaction*, Iterator*, Row*);
	void        (*delete)(Index*, Transaction*, Iterator*);
	void        (*delete_by)(Index*, Transaction*, Row*);
	bool        (*upsert)(Index*, Transaction*, Iterator**, Row*);
	Iterator*   (*open)(Index*, Row*, bool);

	Uuid*        id_storage;
	Uuid*        id_table;
	IndexConfig* config;
	List         link;
};

static inline void
index_init(Index* self, Uuid* id_table, Uuid* id_storage)
{
	self->id_table   = id_table;
	self->id_storage = id_storage;
	memset(self, 0, sizeof(*self));
	list_init(&self->link);
}

static inline void
index_free(Index* self)
{
	self->free(self);
}

static inline bool
index_set(Index* self, Transaction* trx, Row* row)
{
	return self->set(self, trx, row);
}

static inline void
index_update(Index* self, Transaction* trx, Iterator* it, Row* row)
{
	self->update(self, trx, it, row);
}

static inline void
index_delete(Index* self, Transaction* trx, Iterator* it)
{
	self->delete(self, trx, it);
}

static inline void
index_delete_by(Index* self, Transaction* trx, Row* key)
{
	self->delete_by(self, trx, key);
}

static inline bool
index_upsert(Index* self, Transaction* trx, Iterator** it, Row* row)
{
	return self->upsert(self, trx, it, row);
}

static inline Iterator*
index_open(Index* self, Row* key, bool start)
{
	return self->open(self, key, start);
}

static inline Def*
index_def(Index* self)
{
	return &self->config->def;
}
