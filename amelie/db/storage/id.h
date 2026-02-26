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
	ID_PART,
	ID_PART_INCOMPLETE,
	ID_PART_SNAPSHOT,
	ID_PENDING,
	ID_PENDING_INCOMPLETE,
	ID_PENDING_SNAPSHOT
};

typedef void (*IdFree)(Id*);

struct Id
{
	uint64_t id;
	int      type;
	Volume*  volume;
	IdFree   free_function;
	List     link;
};

static inline void
id_init(Id* self)
{
	memset(self, 0, sizeof(*self));
	list_init(&self->link);
}

static inline void
id_free(Id* self)
{
	if (self->free_function)
		self->free_function(self);
}

static inline void
id_set_free(Id* self, IdFree function)
{
	self->free_function = function;
}

static inline void
id_prepare_in(Id* self, int type, Volume* volume)
{
	self->id     = state_psn_next();
	self->type   = type;
	self->volume = volume;
}

static inline void
id_prepare(Id* self, int type, VolumeMgr* volumes)
{
	auto volume = volume_mgr_next(volumes);
	id_prepare_in(self, type, volume);
}

static inline void
id_copy(Id* self, Id* id)
{
	self->id     = id->id;
	self->type   = id->type;
	self->volume = id->volume;
}

static inline int
id_of(const char* name, int64_t* id)
{
	// <id>.service
	// <id>.service.incomplete
	// <id>.partition
	// <id>.partition.incomplete
	// <id>.partition.snapshot
	// <id>.pending
	// <id>.pending.incomplete
	// <id>.pending.snapshot
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
	if (! strcmp(name, ".partition.incomplete"))
		state = ID_PART_INCOMPLETE;
	else
	if (! strcmp(name, ".partition.snapshot"))
		state = ID_PART_SNAPSHOT;
	else
	if (! strcmp(name, ".partition"))
		state = ID_PART;
	else
	if (! strcmp(name, ".pending.incomplete"))
		state = ID_PENDING_INCOMPLETE;
	else
	if (! strcmp(name, ".pending.snapshot"))
		state = ID_PENDING_SNAPSHOT;
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
	case ID_PART:               return ".partition";
	case ID_PART_INCOMPLETE:    return ".partition.incomplete";
	case ID_PART_SNAPSHOT:      return ".partition.snapshot";
	case ID_PENDING:            return ".pending";
	case ID_PENDING_INCOMPLETE: return ".pending.incomplete";
	case ID_PENDING_SNAPSHOT:   return ".pending.snapshot";
	}
	abort();
	return NULL;
}

static inline void
id_path(Id* self, char* path, int state)
{
	if (! self->volume)
	{
		// <base>/storage/<id>.<type>
		sfmt(path, PATH_MAX, "%s/storage/%05" PRIu64 "%s", state_directory(),
		     self->id, id_extension_of(state));
		return;
	}

	// volume id
	char uuid[UUID_SZ];
	uuid_get(&self->volume->id, uuid, sizeof(uuid));

	// <base>/storage/<volume_id>/<id>.<type>
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

	// <base>/storage/<volume_id>/<id>.type
	char path[PATH_MAX];
	id_path(self, path, state);

	// <base>/storage/<volume_id>/<id>.snapshot
	char path_snapshot[PATH_MAX];
	id_path(self, path_snapshot, state_snapshot);

	// hard link
	auto rc = link(path, path_snapshot);
	if (rc == -1)
		error_system();
}

static inline int
id_snapshot_of(int state)
{
	switch (state) {
	case ID_PART:    return ID_PART_SNAPSHOT;
	case ID_PENDING: return ID_PENDING_SNAPSHOT;
	default:
		abort();
	}
	return ID_NONE;
}

static inline void
id_encode(Id* self, int state, Buf* buf)
{
	char uuid[UUID_SZ];
	uuid_get(&self->volume->id, uuid, sizeof(uuid));

	char path[PATH_MAX];
	sfmt(path, sizeof(path), "storage/%s/%05" PRIu64 "%s",
	     uuid, self->id, id_extension_of(state));

	// [path, size, mode]
	encode_basefile(buf, path);
}

static inline void
id_encode_path(Id* self, int state, Buf* buf)
{
	char uuid[UUID_SZ];
	uuid_get(&self->volume->id, uuid, sizeof(uuid));

	char path[PATH_MAX];
	sfmt(path, sizeof(path), "storage/%s/%05" PRIu64 "%s",
	     uuid, self->id, id_extension_of(state));

	// path
	encode_basepath(buf, path);
}
