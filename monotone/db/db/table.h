#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Table Table;

struct Table
{
	Handle       handle;
	TableConfig* config;
};

static inline void
table_free(Table* self)
{
	if (self->config)
		table_config_free(self->config);
	mn_free(self);
}

static inline Table*
table_allocate(TableConfig* config)
{
	Table* self = mn_malloc(sizeof(Table));
	self->config = NULL;
	guard(self_guard, table_free, self);
	self->config = table_config_copy(config);

	handle_init(&self->handle);
	handle_set_schema(&self->handle, &self->config->schema);
	handle_set_name(&self->handle, &self->config->name);
	handle_set_free_function(&self->handle, (HandleFree)table_free);
	return unguard(&self_guard);
}

static inline Table*
table_of(Handle* handle)
{
	return (Table*)handle;
}

static inline Def*
table_def(Table* self)
{
	return &self->config->def;
}
