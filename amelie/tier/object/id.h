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

typedef struct Id Id;

enum
{
	ID_NONE = 0,
	ID      = 1 << 0
};

struct Id
{
	uint64_t id;
	uint64_t id_parent;
	Uuid     id_table;
} packed;

static inline void
id_path(Id* self, Source* source, int state, char* path)
{
	switch (state) {
	case ID:
		// <source_path>/<table_uuid>/<id_parent>.<id>
		source_path(source, path, &self->id_table,
		            "%020" PRIu64 ".%020" PRIu64,
		            self->id_parent,
		            self->id);
		break;
	default:
		abort();
		break;
	}
}
