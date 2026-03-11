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
	ID_PARTITION,
	ID_PARTITION_INCOMPLETE,
	ID_PARTITION_SNAPSHOT
};

struct Id
{
	uint64_t id;
	Volume*  volume;
};

static inline void
id_init(Id* self)
{
	memset(self, 0, sizeof(*self));
}

static inline int
id_state_of(const char* name)
{
	if (! strcmp(name, ".partition"))
		return ID_PARTITION;
	if (! strcmp(name, ".partition.incomplete"))
		return ID_PARTITION_INCOMPLETE;
	if (! strcmp(name, ".partition.snapshot"))
		return ID_PARTITION_SNAPSHOT;
	return -1;
}

static inline const char*
id_state(int state)
{
	switch (state) {
	case ID_PARTITION:            return ".partition";
	case ID_PARTITION_INCOMPLETE: return ".partition.incomplete";
	case ID_PARTITION_SNAPSHOT:   return ".partition.snapshot";
	}
	abort();
	return NULL;
}

static inline int
id_of(const char* name, int64_t* id)
{
	// <id>.<state>
	*id = 0;
	while (*name && *name != '.')
	{
		if (unlikely(! isdigit(*name)))
			return -1;
		if (unlikely(int64_mul_add_overflow(id, *id, 10, *name - '0')))
			return -1;
		name++;
	}
	return id_state_of(name);
}

static inline void
id_path(Id* self, char* path, int state, bool relative)
{
	// volume id
	char uuid[UUID_SZ];
	uuid_get(&self->volume->id, uuid, sizeof(uuid));

	// storage/<volume_id>/<id>.<state>
	if (relative)
	{
		sfmt(path, PATH_MAX, "storage/%s/%05" PRIu64 "%s",
		     uuid, self->id, id_state(state));
		return;
	}

	// <base>/storage/<volume_id>/<id>.<state>
	sfmt(path, PATH_MAX, "%s/storage/%s/%05" PRIu64 "%s",
	     state_directory(),
	     uuid, self->id, id_state(state));
}

static inline void
id_create(Id* self, File* file, int state)
{
	char path[PATH_MAX];
	id_path(self, path, state, false);
	file_create(file, path);
}

static inline void
id_open(Id* self, File* file, int state)
{
	char path[PATH_MAX];
	id_path(self, path, state, false);
	file_open(file, path);
}

static inline void
id_delete(Id* self, int state)
{
	char path[PATH_MAX];
	id_path(self, path, state, false);
	if (fs_exists("%s", path))
		fs_unlink("%s", path);
}

static inline void
id_rename(Id* self, int from, int to)
{
	// rename file from one state to another
	char path_from[PATH_MAX];
	char path_to[PATH_MAX];
	id_path(self, path_from, from, false);
	id_path(self, path_to, to, false);
	if (fs_exists("%s", path_from))
		fs_rename(path_from, "%s", path_to);
}

static inline void
id_snapshot(Id* self, int state, int state_snapshot)
{
	// create file snapshot

	// <base>/storage/<volume_id>/<id>.<state>
	char path[PATH_MAX];
	id_path(self, path, state, false);

	// <base>/storage/<volume_id>/<id>.<state>.snapshot
	char path_snapshot[PATH_MAX];
	id_path(self, path_snapshot, state_snapshot, false);

	// hard link
	auto rc = link(path, path_snapshot);
	if (rc == -1)
		error_system();
}

static inline void
id_encode(Id* self, int state, Buf* buf)
{
	char path[PATH_MAX];
	id_path(self, path, state, true);
	encode_basefile(buf, path);
}

static inline void
id_encode_path(Id* self, int state, Buf* buf)
{
	char path[PATH_MAX];
	id_path(self, path, state, true);
	encode_basepath(buf, path);
}
