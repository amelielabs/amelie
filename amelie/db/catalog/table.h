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
	TierMgr      tier_mgr;
	Deploy*      deploy;
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
table_free(Table* self)
{
	deploy_detach_all(self->deploy, &self->part_mgr);
	part_mgr_free(&self->part_mgr);
	tier_mgr_free(&self->tier_mgr);
	sequence_free(&self->seq);
	if (self->config)
		table_config_free(self->config);
	am_free(self);
}

static inline Table*
table_allocate(TableConfig* config, StorageMgr* storage_mgr, Deploy* deploy)
{
	Table* self = am_malloc(sizeof(Table));
	self->config = table_config_copy(config);
	self->deploy = deploy;
	sequence_init(&self->seq);
	auto primary = table_primary(self);
	tier_mgr_init(&self->tier_mgr, storage_mgr, &self->config->id);
	part_mgr_init(&self->part_mgr, &self->tier_mgr, &self->seq,
	              self->config->unlogged,
	              &primary->keys);

	relation_init(&self->rel);
	relation_set_db(&self->rel, &self->config->db);
	relation_set_name(&self->rel, &self->config->name);
	relation_set_free_function(&self->rel, (RelationFree)table_free);
	return self;
}

static inline void
table_open(Table* self)
{
	// recover hash partitions
	part_mgr_open(&self->part_mgr, &self->config->indexes);

	// recover tiers and objects
	tier_mgr_open(&self->tier_mgr, &self->config->tiers);

	// create pods
	deploy_attach_all(self->deploy, &self->part_mgr);
}

static inline void
table_set_unlogged(Table* self, bool value)
{
	table_config_set_unlogged(self->config, value);
	part_mgr_set_unlogged(&self->part_mgr, value);
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
		if (str_compare_case(&config->name, name))
			return config;
	}
	if (error_if_not_exists)
		error("index '%.*s': not exists", str_size(name),
		       str_of(name));
	return NULL;
}
