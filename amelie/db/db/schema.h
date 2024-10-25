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
	Handle        handle;
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
	handle_init(&self->handle);
	handle_set_schema(&self->handle, NULL);
	handle_set_name(&self->handle, &self->config->name);
	handle_set_free_function(&self->handle, (HandleFree)schema_free);
	return self;
}

static inline Schema*
schema_of(Handle* handle)
{
	return (Schema*)handle;
}
