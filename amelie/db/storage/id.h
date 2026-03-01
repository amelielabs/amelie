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
	STATE_COMPLETE,
	STATE_INCOMPLETE,
	STATE_SNAPSHOT,
};

struct Id
{
	uint64_t id;
	uint64_t version;
	Volume*  volume;
};

static inline int
id_state_of(const char* name)
{
	if (! *name)
		return STATE_COMPLETE;
	if (! strcmp(name, ".incomplete"))
		return STATE_INCOMPLETE;
	if (! strcmp(name, ".snapshot"))
		return STATE_SNAPSHOT;
	return -1;
}

static inline const char*
id_state(int state)
{
	switch (state) {
	case STATE_COMPLETE:   return "";
	case STATE_INCOMPLETE: return ".incomplete";
	case STATE_SNAPSHOT:   return ".snapshot";
	}
	abort();
	return NULL;
}

static inline const char*
id_of_next(const char* name, int64_t* id)
{
	while (*name && *name != '.')
	{
		if (unlikely(! isdigit(*name)))
			return NULL;
		if (unlikely(int64_mul_add_overflow(id, *id, 10, *name - '0')))
			return NULL;
		name++;
	}
	return name;
}

static inline int
id_of(const char* name, int64_t* id, int64_t* version)
{
	// <id>[.<version>][.state]
	*id      = 0;
	*version = 0;

	// <id>[.]
	name = id_of_next(name, id);
	if (! name)
		return -1;
	if (! *name)
		return STATE_COMPLETE;

	// .
	name++;
	if (unlikely(! *name))
		return -1;

	// <version>[.]
	if (isdigit(*name))
	{
		name = id_of_next(name, version);
		if (! name)
			return -1;
	}
	if (unlikely(! *name))
		return STATE_COMPLETE;

	return id_state_of(name);
}

static inline void
id_path(Id* self, char* path, int state, bool relative)
{
	// volume id
	char uuid[UUID_SZ];
	uuid_get(&self->volume->id, uuid, sizeof(uuid));

	// storage/<volume_id>/<id>.<version>[.state]
	// storage/<volume_id>/<id>[.<state>]
	if (relative)
	{
		if (self->version > 0)
			sfmt(path, PATH_MAX, "storage/%s/%05" PRIu64 ".%02" PRIu64 "%s",
			     uuid, self->id, self->version, id_state(state));
		else
			sfmt(path, PATH_MAX, "storage/%s/%05" PRIu64 "%s",
			     uuid, self->id, id_state(state));
		return;
	}

	// <base>/storage/<volume_id>/<id>.<version>[.state]
	// <base>/storage/<volume_id>/<id>[.<state>]
	if (self->version > 0)
		sfmt(path, PATH_MAX, "%s/storage/%s/%05" PRIu64 ".%02" PRIu64 "%s",
		     state_directory(),
		     uuid, self->id, self->version, id_state(state));
	else
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

	// <base>/storage/<volume_id>/<id>[.<version>]
	char path[PATH_MAX];
	id_path(self, path, state, false);

	// <base>/storage/<volume_id>/<id>[.<version>].snapshot
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
