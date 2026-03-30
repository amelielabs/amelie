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
	Rel          rel;
	PartMgr      part_mgr;
	PartArg      part_arg;
	Sequence     seq;
	SnapshotMgr  snapshot_mgr;
	TableConfig* config;
};

static inline IndexConfig*
table_primary(Table* self)
{
	return container_of(self->config->indexes.next, IndexConfig, link);
}

static inline Snapshot*
table_main(Table* self)
{
	return &self->snapshot_mgr.main;
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
	arg->seq       = &self->seq;
	arg->unlogged  =  self->config->unlogged;
	arg->id_table  = &self->config->id;
	arg->snapshots = &self->snapshot_mgr;
	arg->config    =  &self->config->partitioning;

	// partition manager
	auto primary = table_primary(self);
	part_mgr_init(&self->part_mgr, iface, iface_arg,
	              &self->config->partitioning, arg, storage_mgr,
	              &primary->keys);

	// snapshot manager
	snapshot_mgr_init(&self->snapshot_mgr);

	// set relation
	auto rel = &self->rel;
	rel_init(rel, REL_TABLE);
	rel_set_user(rel, &self->config->user);
	rel_set_name(rel, &self->config->name);
	rel_set_grants(rel, &self->config->grants);
	rel_set_free_function(rel, (RelFree)table_free);
	rel_set_rsn(rel, state_rsn_next());
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
table_of(Rel* self)
{
	return (Table*)self;
}

static inline Snapshot*
table_find_snapshot(Table* self, int id, bool error_if_not_exists)
{
	auto snapshot = snapshot_mgr_find(&self->snapshot_mgr, id);
	if (! snapshot)
	{
		if (error_if_not_exists)
			error("table '%.*s': snapshot %d not found",
			      str_size(&self->config->name),
			      str_of(&self->config->name),
			      id);
	}

	return snapshot;
}
