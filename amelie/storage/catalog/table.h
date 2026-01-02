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
	VolumeMgr    volume_mgr;
	Deploy*      deploy;
	Sequence     seq;
	TableConfig* config;
};

static inline void
table_free(Table* self)
{
	deploy_detach(self->deploy, &self->volume_mgr);
	volume_mgr_free(&self->volume_mgr);
	if (self->config)
		table_config_free(self->config);
	sequence_free(&self->seq);
	am_free(self);
}

static inline Table*
table_allocate(TableConfig* config, TierMgr* tier_mgr, Deploy* deploy)
{
	Table* self = am_malloc(sizeof(Table));
	self->config = table_config_copy(config);
	self->deploy = deploy;
	sequence_init(&self->seq);
	volume_mgr_init(&self->volume_mgr, &config->uuid, tier_mgr,
	                &self->seq,
	                 self->config->unlogged);
	relation_init(&self->rel);
	relation_set_db(&self->rel, &self->config->db);
	relation_set_name(&self->rel, &self->config->name);
	relation_set_free_function(&self->rel, (RelationFree)table_free);
	return self;
}

static inline void
table_open(Table* self)
{
	volume_mgr_open(&self->volume_mgr, &self->config->volumes,
	                &self->config->indexes);

	deploy_attach(self->deploy, &self->volume_mgr);
}

static inline void
table_set_unlogged(Table* self, bool value)
{
	table_config_set_unlogged(self->config, value);
	volume_mgr_set_unlogged(&self->volume_mgr, value);
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
