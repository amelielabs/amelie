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

static inline void
storage_path(Storage* self, char* buf, Id* id, int state)
{
	// tier id (uuid)
	char id_tier[UUID_SZ];
	uuid_get(&id->id_tier, id_tier, sizeof(id_tier));

	switch (state) {
	case ID:
		// <storage_path>/<id_tier>/<id_part>/<id>
		storage_fmt(self, buf,
		            "%s/%05" PRIu64 "/%05" PRIu64,
		            id_tier,
		            id->id_part,
		            id->id);
		break;
	case ID_INCOMPLETE:
		// <storage_path>/<id_tier>/<id_part>/<id>.incomplete
		storage_fmt(self, buf,
		            "%s/%05" PRIu64 "/%05" PRIu64 ".incomplete",
		            id_tier,
		            id->id_part,
		            id->id);
		break;
	default:
		abort();
		break;
	}
}

void
storage_create(Storage* self, File* file, Id* id, int state)
{
	char path[PATH_MAX];
	storage_path(self, path, id, state);
	file_create(file, path);
}

void
storage_delete(Storage* self, Id* id, int state)
{
	// <source_path>/<table_uuid>/<id_parent>.<id>
	char path[PATH_MAX];
	storage_path(self, path, id, state);
	if (fs_exists("%s", path))
		fs_unlink("%s", path);
}

void
storage_rename(Storage* self, Id* id, int from, int to)
{
	// rename file from one state to another
	char path_from[PATH_MAX];
	char path_to[PATH_MAX];
	storage_path(self, path_from, id, from);
	storage_path(self, path_to, id, to);
	if (fs_exists("%s", path_from))
		fs_rename(path_from, "%s", path_to);
}
