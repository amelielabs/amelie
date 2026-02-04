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

typedef struct Database Database;

struct Database
{
	Relation        rel;
	DatabaseConfig* config;
};

static inline void
database_free(Database* self, bool drop)
{
	unused(drop);
	if (self->config)
		database_config_free(self->config);
	am_free(self);
}

static inline Database*
database_allocate(DatabaseConfig* config)
{
	Database* self = am_malloc(sizeof(Database));
	self->config = database_config_copy(config);
	relation_init(&self->rel);
	relation_set_db(&self->rel, NULL);
	relation_set_name(&self->rel, &self->config->name);
	relation_set_free_function(&self->rel, (RelationFree)database_free);
	return self;
}

static inline Database*
database_of(Relation* self)
{
	return (Database*)self;
}
