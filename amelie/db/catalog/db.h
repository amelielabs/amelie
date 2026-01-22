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

typedef struct Db Db;

struct Db
{
	Relation  rel;
	DbConfig* config;
};

static inline void
db_free(Db* self)
{
	if (self->config)
		db_config_free(self->config);
	am_free(self);
}

static inline Db*
db_allocate(DbConfig* config)
{
	Db* self = am_malloc(sizeof(Db));
	self->config = db_config_copy(config);
	relation_init(&self->rel);
	relation_set_db(&self->rel, NULL);
	relation_set_name(&self->rel, &self->config->name);
	relation_set_free_function(&self->rel, (RelationFree)db_free);
	return self;
}

static inline Db*
db_of(Relation* self)
{
	return (Db*)self;
}
