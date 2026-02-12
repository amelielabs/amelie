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
	ID_NONE,
	ID_SERVICE,
	ID_SERVICE_INCOMPLETE,
	ID_RAM,
	ID_RAM_INCOMPLETE
};

struct Id
{
	uint64_t     id;
	Uuid         id_tier;
	Uuid         id_table;
	TierStorage* storage;
	Tier*        tier;
};

static inline void
id_init(Id* self)
{
	memset(self, 0, sizeof(*self));
}

static inline int
id_of(const char* name, int64_t* id)
{
	// <id>.service
	// <id>.service.incomplete
	// <id>.ram
	// <id>.ram.incomplete
	*id = 0;
	while (*name && *name != '.')
	{
		if (unlikely(! isdigit(*name)))
			return -1;
		if (unlikely(int64_mul_add_overflow(id, *id, 10, *name - '0')))
			return -1;
		name++;
	}
	int state;
	if (! strcmp(name, ".service.incomplete"))
		state = ID_SERVICE_INCOMPLETE;
	else
	if (! strcmp(name, ".service"))
		state = ID_SERVICE;
	else
	if (! strcmp(name, ".ram.incomplete"))
		state = ID_RAM_INCOMPLETE;
	else
	if (! strcmp(name, ".ram"))
		state = ID_RAM;
	else
		state = -1;
	return state;
}

static inline const char*
id_extension_of(int state)
{
	switch (state) {
	case ID_SERVICE:            return ".service";
	case ID_SERVICE_INCOMPLETE: return ".service.incomplete";
	case ID_RAM:                return ".ram";
	case ID_RAM_INCOMPLETE:     return ".ram.incomplete";
	}
	abort();
	return NULL;
}

static inline void
id_path(Id* self, char* path, int state)
{
	// tier id (uuid)
	char id_tier[UUID_SZ];
	uuid_get(&self->id_tier, id_tier, sizeof(id_tier));

	// <base>/storage/<storage_name>/<id_tier>
	auto storage = self->storage->storage;
	switch (state) {
	case ID_SERVICE:
		// <storage_path>/<id_tier>/<id>.service
		storage_pathfmt(storage, path, "%s/%05" PRIu64 ".service",
		                id_tier, self->id);
		break;
	case ID_SERVICE_INCOMPLETE:
		// <storage_path>/<id_tier>/<id>.service.incomplete
		storage_pathfmt(storage, path, "%s/%05" PRIu64 ".service.incomplete",
		                id_tier, self->id);
		break;
	case ID_RAM:
		// <storage_path>/<id_tier>/<id>.ram
		storage_pathfmt(storage, path, "%s/%05" PRIu64 ".ram",
		                id_tier, self->id);
		break;
	case ID_RAM_INCOMPLETE:
		// <storage_path>/<id_tier>/<id>.ram.incomplete
		storage_pathfmt(storage, path, "%s/%05" PRIu64 ".ram.incomplete",
		                id_tier, self->id);
		break;
	default:
		abort();
		break;
	}
}

static inline void
id_encode(Id* self, int state, Buf* buf)
{
	encode_obj(buf);

	// id
	encode_raw(buf, "id", 2);
	encode_integer(buf, self->id);

	// state
	encode_raw(buf, "state", 5);
	encode_integer(buf, state);

	// storage
	encode_raw(buf, "storage", 7);
	encode_string(buf, &self->storage->storage->config->name);

	// tier
	encode_raw(buf, "tier", 4);
	encode_uuid(buf, &self->id_tier);

	encode_obj_end(buf);
}

static inline void
id_create(Id* self, File* file, int state)
{
	char path[PATH_MAX];
	id_path(self, path, state);
	file_create(file, path);
}

static inline void
id_open(Id* self, File* file, int state)
{
	char path[PATH_MAX];
	id_path(self, path, state);
	file_open(file, path);
}

static inline void
id_delete(Id* self, int state)
{
	char path[PATH_MAX];
	id_path(self, path, state);
	if (fs_exists("%s", path))
		fs_unlink("%s", path);
}

static inline void
id_rename(Id* self, int from, int to)
{
	// rename file from one state to another
	char path_from[PATH_MAX];
	char path_to[PATH_MAX];
	id_path(self, path_from, from);
	id_path(self, path_to, to);
	if (fs_exists("%s", path_from))
		fs_rename(path_from, "%s", path_to);
}

static inline void
id_snapshot(Id* self, int state)
{
	// create file snapshot in the corresponding storage
	// snapshot directory

	// <storage_path>/<id_tier>/<id>.extension
	char path[PATH_MAX];
	id_path(self, path, state);

	// <storage_path>/snapshot/<id>.extension
	auto storage = self->storage->storage;
	char path_snapshot[PATH_MAX];
	storage_pathfmt(storage, path_snapshot, "snapshot/%05" PRIu64 "%s", self->id,
	                id_extension_of(state));

	// hard link
	auto rc = link(path, path_snapshot);
	if (rc == -1)
		error_system();
}
