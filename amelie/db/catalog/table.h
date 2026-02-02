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
	Engine       engine;
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
	engine_close(&self->engine);
	engine_free(&self->engine);
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
	sequence_init(&self->seq);
	auto primary = table_primary(self);
	engine_init(&self->engine, storage_mgr, deploy, &self->config->id,
	            &self->seq, self->config->unlogged,
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
	// resolve storages on the tiers
	list_foreach(&self->config->tiers)
	{
		auto tier = list_at(Tier, link);
		tier_resolve(tier, self->engine.storage_mgr);
	}

	// recover, map and deploy partitions
	auto config = self->config;
	engine_open(&self->engine, &config->tiers, &config->indexes,
	            config->partitions);
}

static inline void
table_set_unlogged(Table* self, bool value)
{
	table_config_set_unlogged(self->config, value);
	engine_set_unlogged(&self->engine, value);
}

static inline Table*
table_of(Relation* self)
{
	return (Table*)self;
}
