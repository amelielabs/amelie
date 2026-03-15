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
	Rel             rel;
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

	// set relation
	auto rel = &self->rel;
	rel_init(rel);
	rel_set_db(rel, NULL);
	rel_set_name(rel, &self->config->name);
	rel_set_free_function(rel, (RelFree)database_free);
	rel_set_rsn(rel, state_rsn_next());
	return self;
}

static inline Database*
database_of(Rel* self)
{
	return (Database*)self;
}
