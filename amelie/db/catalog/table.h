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
	Parts        parts;
	PartArg      part_arg;
	Timelines    timelines;
	TableConfig* config;
};

bool table_create(Catalog*, Tr*, TableConfig*, bool);
bool table_truncate(Catalog*, Tr*, Str*, Str*, bool);

static inline void
table_sync(Table* self)
{
	// note: assuming exclusive lock

	// force commit pending prepared transactions
	list_foreach(&self->parts.list)
	{
		auto part = list_at(Part, link);
		track_sync(&part->track, &part->track.consensus);
	}
}

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

static inline Timeline*
table_main(Table* self)
{
	return &self->timelines.main;
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
