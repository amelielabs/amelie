#pragma once

//
// sonata.
//
// SQL Database for JSON.
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
	so_free(self);
}

static inline Schema*
schema_allocate(SchemaConfig* config)
{
	Schema* self = so_malloc(sizeof(Schema));
	self->config = NULL;
	guard(schema_free, self);
	self->config = schema_config_copy(config);

	handle_init(&self->handle);
	handle_set_schema(&self->handle, NULL);
	handle_set_name(&self->handle, &self->config->name);
	handle_set_free_function(&self->handle, (HandleFree)schema_free);
	return unguard();
}

static inline Schema*
schema_of(Handle* handle)
{
	return (Schema*)handle;
}
