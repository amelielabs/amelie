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
	ID_NONE       = 0,
	ID            = 1 << 0,
	ID_INCOMPLETE = 1 << 1,
	ID_COMPLETE   = 1 << 2
};

struct Id
{
	uint64_t id;
	Uuid     id_table;
} packed;

static inline void
id_path(Id* self, Source* source, int state, char* path)
{
	switch (state) {
	case ID:
		// <source_path>/<table_uuid>/<id>
		source_path(source, path, &self->id_table,
		            "%020" PRIu64, self->id);
		break;
	case ID_INCOMPLETE:
		// <source_path>/<table_uuid>/<id>.incomplete
		source_path(source, path, &self->id_table,
		            "%020" PRIu64 ".incomplete", self->id);
		break;
	case ID_COMPLETE:
		// <source_path>/<table_uuid>/<id>.complete
		source_path(source, path, &self->id_table,
		            "%020" PRIu64 ".complete", self->id);
		break;
	default:
		abort();
		break;
	}
}
