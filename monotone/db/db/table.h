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
	Storage*     storage;
	TableConfig* config;
};

static inline void
table_free(Table* self)
{
	if (self->storage)
		storage_unref(self->storage);
	if (self->config)
		table_config_free(self->config);
	mn_free(self);
}

static inline Table*
table_allocate(TableConfig* config)
{
	Table* self = mn_malloc(sizeof(Table));
	self->storage = NULL;
	self->config  = NULL;
	guard(self_guard, table_free, self);
	self->config  = table_config_copy(config);
	self->storage = NULL;
	handle_init(&self->handle);
	self->handle.name = &self->config->name;
	return unguard(&self_guard);
}

static inline void
table_destructor(Handle* handle, void* arg)
{
	unused(arg);
	auto self = (Table*)handle;
	table_free(self);
}

static inline Table*
table_of(Handle* handle)
{
	return (Table*)handle;
}

static inline Schema*
table_schema(Table* self)
{
	return &self->config->config->schema;
}
