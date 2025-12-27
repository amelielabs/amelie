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
	Uuid     uuid;
} packed;

static inline void
id_path(Id* self, Source* source, int state, char* path)
{
	char uuid[UUID_SZ];
	uuid_get(&self->uuid, uuid, sizeof(uuid));
	switch (state) {
	case ID:
		// <source_path>/<uuid>/<psn>
		source_fmt(source, path, "%s/%020" PRIu64,
		           uuid, self->id);
		break;
	case ID_INCOMPLETE:
		// <source_path>/<uuid>/<psn>.incomplete
		source_fmt(source, path, "%s/%020" PRIu64 ".incomplete",
		           uuid, self->id);
		break;
	case ID_COMPLETE:
		// <source_path>/<uuid>/<psn>.complete
		source_fmt(source, path, "%s/%020" PRIu64 ".complete",
		           uuid, self->id);
		break;
	default:
		abort();
		break;
	}
}
