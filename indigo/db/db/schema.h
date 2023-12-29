#pragma once

//
// indigo
//
// SQL OLTP database
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
	mn_free(self);
}

static inline Schema*
schema_allocate(SchemaConfig* config)
{
	Schema* self = mn_malloc(sizeof(Schema));
	self->config = NULL;
	guard(self_guard, schema_free, self);
	self->config = schema_config_copy(config);

	handle_init(&self->handle);
	handle_set_schema(&self->handle, NULL);
	handle_set_name(&self->handle, &self->config->name);
	handle_set_free_function(&self->handle, (HandleFree)schema_free);
	return unguard(&self_guard);
}

static inline Schema*
schema_of(Handle* handle)
{
	return (Schema*)handle;
}
