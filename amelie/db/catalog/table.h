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
	PartMgr      part_mgr;
	PartArg      part_arg;
	Sequence     seq;
	TableConfig* config;
};

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

static inline void
table_free(Table* self, bool drop)
{
	auto part_mgr = &self->part_mgr;
	part_mgr_close(part_mgr);
	if (drop)
		part_mgr_drop(part_mgr);
	part_mgr_free(part_mgr);

	sequence_free(&self->seq);
	if (self->config)
		table_config_free(self->config);
	am_free(self);
}

static inline Table*
table_allocate(TableConfig* config,
               StorageMgr*  storage_mgr,
               PartMgrIf*   iface,
               void*        iface_arg)
{
	auto self = (Table*)am_malloc(sizeof(Table));
	self->config = table_config_copy(config);
	sequence_init(&self->seq);

	// part context
	auto arg = &self->part_arg;
	arg->seq      = &self->seq;
	arg->unlogged =  self->config->unlogged;
	arg->id_table = &self->config->id;
	arg->config   =  &self->config->partitioning;

	// partition manager
	auto primary = table_primary(self);
	part_mgr_init(&self->part_mgr, iface, iface_arg,
	              &self->config->partitioning, arg, storage_mgr,
	              &primary->keys);

	relation_init(&self->rel);
	relation_set_db(&self->rel, &self->config->db);
	relation_set_name(&self->rel, &self->config->name);
	relation_set_free_function(&self->rel, (RelationFree)table_free);
	relation_set_rsn(&self->rel, state_rsn_next());
	return self;
}

static inline void
table_open(Table* self)
{
	// recover, map and deploy partitions
	part_mgr_open(&self->part_mgr, &self->config->indexes);
}

static inline void
table_set_unlogged(Table* self, bool value)
{
	table_config_set_unlogged(self->config, value);
	self->part_arg.unlogged = value;
}

static inline Table*
table_of(Relation* self)
{
	return (Table*)self;
}
