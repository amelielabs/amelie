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

typedef struct Table Table;

struct Table
{
	Handle       handle;
	PartList     part_list;
	Sequence     seq;
	TableConfig* config;
};

static inline void
table_free(Table* self)
{
	part_list_free(&self->part_list);
	if (self->config)
		table_config_free(self->config);
	sequence_free(&self->seq);
	am_free(self);
}

static inline Table*
table_allocate(TableConfig* config, PartMgr* part_mgr)
{
	Table* self = am_malloc(sizeof(Table));
	self->config = table_config_copy(config);
	sequence_init(&self->seq);
	part_list_init(&self->part_list, part_mgr);
	handle_init(&self->handle);
	handle_set_schema(&self->handle, &self->config->schema);
	handle_set_name(&self->handle, &self->config->name);
	handle_set_free_function(&self->handle, (HandleFree)table_free);
	return self;
}

static inline void
table_open(Table* self)
{
	part_list_create(&self->part_list,
	                  self->config->shared,
	                  self->config->unlogged,
	                 &self->seq,
	                 &self->config->partitions,
	                 &self->config->indexes);
}

static inline void
table_set_unlogged(Table* self, bool value)
{
	table_config_set_unlogged(self->config, value);
	part_list_set_unlogged(&self->part_list, value);
}

static inline Table*
table_of(Handle* handle)
{
	return (Table*)handle;
}

hot static inline IndexConfig*
table_find_index(Table* self, Str* name, bool error_if_not_exists)
{
	list_foreach(&self->config->indexes)
	{
		auto config = list_at(IndexConfig, link);
		if (str_compare(&config->name, name))
			return config;
	}
	if (error_if_not_exists)
		error("index '%.*s': not exists", str_size(name),
		       str_of(name));
	return NULL;
}

static inline IndexConfig*
table_primary(Table* self)
{
	return container_of(self->config->indexes.next, IndexConfig, link);
}

static inline Columns*
table_columns(Table* self)
{
	return &self->config->columns;
}

static inline Keys*
table_keys(Table* self)
{
	return &table_primary(self)->keys;
}
