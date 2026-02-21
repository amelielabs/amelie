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
	ID_RAM_INCOMPLETE,
	ID_RAM_SNAPSHOT,
	ID_PENDING,
	ID_PENDING_INCOMPLETE
};

struct Id
{
	uint64_t     id;
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
	// <id>.ram.snapshot
	// <id>.pending
	// <id>.pending.incomplete
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
	if (! strcmp(name, ".ram.snapshot"))
		state = ID_RAM_SNAPSHOT;
	else
	if (! strcmp(name, ".ram"))
		state = ID_RAM;
	else
	if (! strcmp(name, ".pending.incomplete"))
		state = ID_PENDING_INCOMPLETE;
	else
	if (! strcmp(name, ".pending"))
		state = ID_PENDING;
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
	case ID_RAM_SNAPSHOT:       return ".ram.snapshot";
	case ID_PENDING_INCOMPLETE: return ".pending.incomplete";
	case ID_PENDING:            return ".pending";
	}
	abort();
	return NULL;
}

static inline void
id_path(Id* self, char* path, int state)
{
	// tier storage id (uuid)
	char uuid[UUID_SZ];
	uuid_get(&self->storage->id, uuid, sizeof(uuid));

	// <base>/storage/<id_tier_storage>/<id>.<type>
	sfmt(path, PATH_MAX, "%s/storage/%s/%05" PRIu64 "%s", state_directory(),
	     uuid, self->id, id_extension_of(state));
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
id_snapshot(Id* self, int state, int state_snapshot)
{
	// create file snapshot

	// <base>/storage/<id_tier_storage>/<id>.type
	char path[PATH_MAX];
	id_path(self, path, state);

	// <base>/storage/<id_tier_storage>/<id>.snapshot
	char path_snapshot[PATH_MAX];
	id_path(self, path_snapshot, state_snapshot);

	// hard link
	auto rc = link(path, path_snapshot);
	if (rc == -1)
		error_system();
}

static inline void
id_encode(Id* self, int state, Buf* buf)
{
	char uuid[UUID_SZ];
	uuid_get(&self->storage->id, uuid, sizeof(uuid));

	char path[PATH_MAX];
	sfmt(path, sizeof(path), "storage/%s/%05" PRIu64 "%s",
	     uuid, self->id, id_extension_of(state));

	// [path, size, mode]
	encode_basefile(buf, path);
}

static inline void
id_status(Id*      self, int state, Buf* buf, bool extended,
          int      hash_min,
          int      hash_max,
          uint64_t lsn,
          uint64_t size,
          int      compression)
{
	unused(extended);
	encode_obj(buf);

	// id
	encode_raw(buf, "id", 2);
	encode_integer(buf, self->id);

	// tier
	encode_raw(buf, "tier", 4);
	encode_string(buf, &self->tier->name);

	// tier
	encode_raw(buf, "storage", 7);
	encode_string(buf, &self->storage->storage->config->name);

	// ram
	encode_raw(buf, "ram", 3);
	encode_bool(buf, state == ID_RAM);

	// pending
	encode_raw(buf, "pending", 7);
	encode_bool(buf, state == ID_PENDING);

	// min
	encode_raw(buf, "min", 3);
	encode_integer(buf, hash_min);

	// max
	encode_raw(buf, "max", 3);
	encode_integer(buf, hash_max);

	// lsn
	encode_raw(buf, "lsn", 3);
	encode_integer(buf, lsn);

	// size
	encode_raw(buf, "size", 4);
	encode_integer(buf, size);

	// compression
	encode_raw(buf, "compression", 11);
	encode_integer(buf, compression);

	encode_obj_end(buf);
}
