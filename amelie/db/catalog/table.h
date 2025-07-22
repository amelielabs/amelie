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
	Relation     rel;
	PartList     part_list;
	PartMgr*     part_mgr;
	Sequence     seq;
	TableConfig* config;
};

static inline void
table_free(Table* self)
{
	part_mgr_detach(self->part_mgr, &self->part_list);
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
	self->part_mgr = part_mgr;
	self->config = table_config_copy(config);
	sequence_init(&self->seq);
	part_list_init(&self->part_list);
	relation_init(&self->rel);
	relation_set_schema(&self->rel, &self->config->schema);
	relation_set_name(&self->rel, &self->config->name);
	relation_set_free_function(&self->rel, (RelationFree)table_free);
	return self;
}

static inline void
table_open(Table* self)
{
	part_list_create(&self->part_list,
	                  self->config->unlogged,
	                 &self->seq,
	                 &self->config->partitions,
	                 &self->config->indexes);

	part_mgr_attach(self->part_mgr, &self->part_list);
}

static inline void
table_set_unlogged(Table* self, bool value)
{
	table_config_set_unlogged(self->config, value);
	part_list_set_unlogged(&self->part_list, value);
}

static inline Table*
table_of(Relation* self)
{
	return (Table*)self;
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
