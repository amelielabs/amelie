#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Index Index;

struct Index
{
	IndexConfig*  config;
	void        (*free)(Index*);
	bool        (*set)(Index*, Transaction*, Row*);
	void        (*update)(Index*, Transaction*, Iterator*, Row*);
	void        (*delete)(Index*, Transaction*, Iterator*);
	void        (*delete_by)(Index*, Transaction*, Row*);
	Iterator*   (*read)(Index*);
	List          link;
};

static inline void
index_init(Index* self)
{
	memset(self, 0, sizeof(*self));
	list_init(&self->link);
}

static inline void
index_free(Index* self)
{
	if (self->config)
		index_config_free(self->config);
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

static inline Iterator*
index_read(Index* self)
{
	return self->read(self);
}

static inline Schema*
index_schema(Index* self)
{
	return &self->config->schema;
}
