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

extern HandleIf table_if;

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
	handle_init(&self->handle);
	handle_set_name(&self->handle, &self->config->name);
	handle_set_iface(&self->handle, &table_if, self);
	guard(self_guard, table_free, self);
	self->config = table_config_copy(config);
	return unguard(&self_guard);
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

static inline void
table_ref(Table* self)
{
	handle_ref(&self->handle);
}

static inline void
table_unref(Table* self)
{
	// free
	handle_unref(&self->handle);
}
