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

bool table_create(Catalog*, Tr*, TableConfig*, bool);
bool table_truncate(Catalog*, Tr*, Str*, Str*, bool);

always_inline static inline Table*
table_of(Rel* self)
{
	return (Table*)self;
}

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

static inline Snapshot*
table_find_snapshot(Table* self, int id, bool error_if_not_exists)
{
	auto snapshot = snapshot_mgr_find(&self->snapshot_mgr, id);
	if (! snapshot)
	{
		if (error_if_not_exists)
			error("table '{str}': snapshot {d} not found",
			      &self->config->name, id);
	}
	return snapshot;
}
