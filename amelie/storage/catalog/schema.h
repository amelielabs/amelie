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

typedef struct Schema Schema;

struct Schema
{
	Relation      rel;
	SchemaConfig* config;
};

static inline void
schema_free(Schema* self)
{
	if (self->config)
		schema_config_free(self->config);
	am_free(self);
}

static inline Schema*
schema_allocate(SchemaConfig* config)
{
	Schema* self = am_malloc(sizeof(Schema));
	self->config = schema_config_copy(config);
	relation_init(&self->rel);
	relation_set_schema(&self->rel, NULL);
	relation_set_name(&self->rel, &self->config->name);
	relation_set_free_function(&self->rel, (RelationFree)schema_free);
	return self;
}

static inline Schema*
schema_of(Relation* self)
{
	return (Schema*)self;
}
