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
	Row*        (*set)(Index*, Transaction*, Row*);
	Row*        (*delete)(Index*, Transaction*, Row*);
	Row*        (*get)(Index*, Row*);
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

static inline Row*
index_set(Index* self, Transaction* trx, Row* row)
{
	return self->set(self, trx, row);
}

static inline Row*
index_delete(Index* self, Transaction* trx, Row* row)
{
	return self->delete(self, trx, row);
}

static inline Row*
index_get(Index* self, Row* row)
{
	return self->get(self, row);
}

static inline Iterator*
index_read(Index* self)
{
	return self->read(self);
}
