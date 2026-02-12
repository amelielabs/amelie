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

typedef struct TierStorage TierStorage;

struct TierStorage
{
	Str      name;
	Uuid     id;
	Storage* storage;
	bool     pause;
	int      refs;
	List     link;
};

static inline TierStorage*
tier_storage_allocate(void)
{
	auto self = (TierStorage*)am_malloc(sizeof(TierStorage));
	self->storage = NULL;
	self->pause   = false;
	self->refs    = 0;
	uuid_init(&self->id);
	str_init(&self->name);
	list_init(&self->link);
	return self;
}

static inline void
tier_storage_free(TierStorage* self)
{
	str_free(&self->name);
	am_free(self);
}

static inline void
tier_storage_ref(TierStorage* self)
{
	self->refs++;
}

static inline void
tier_storage_unref(TierStorage* self)
{
	self->refs--;
	assert(self->refs >= 0);
}

static inline void
tier_storage_set_name(TierStorage* self, Str* name)
{
	str_free(&self->name);
	str_copy(&self->name, name);
}

static inline void
tier_storage_set_id(TierStorage* self, Uuid* id)
{
	self->id = *id;
}

static inline void
tier_storage_set_pause(TierStorage* self, bool value)
{
	self->pause = value;
}

static inline TierStorage*
tier_storage_copy(TierStorage* from)
{
	auto self = tier_storage_allocate();
	tier_storage_set_name(self, &from->name);
	tier_storage_set_id(self, &from->id);
	tier_storage_set_pause(self, from->pause);
	return self;
}

static inline TierStorage*
tier_storage_read(uint8_t** pos)
{
	auto self = tier_storage_allocate();
	errdefer(tier_storage_free, self);
	Decode obj[] =
	{
		{ DECODE_STRING, "name",  &self->name  },
		{ DECODE_UUID,   "id",    &self->id    },
		{ DECODE_BOOL,   "pause", &self->pause },
		{ 0,              NULL,    NULL        },
	};
	decode_obj(obj, "tier_storage", pos);
	return self;
}

static inline void
tier_storage_write(TierStorage* self, Buf* buf, int flags)
{
	// map
	encode_obj(buf);

	// name
	encode_raw(buf, "name", 4);
	encode_string(buf, &self->name);

	// id
	encode_raw(buf, "id", 2);
	encode_uuid(buf, &self->id);

	// pause
	encode_raw(buf, "pause", 5);
	encode_bool(buf, self->pause);

	// partitions
	if (flags_has(flags, FMETRICS))
	{
		encode_raw(buf, "partitions", 10);
		encode_integer(buf, self->refs);
	}

	encode_obj_end(buf);
}

static inline void
tier_storage_mkdir(TierStorage* self)
{
	if (! self->storage)
		return;

	// <base>/storage/<id_tier_storage>
	char id[UUID_SZ];
	uuid_get(&self->id, id, sizeof(id));

	char path[PATH_MAX];
	sfmt(path, PATH_MAX, "%s/storage/%s", state_directory(), id);
	if (fs_exists("%s", path))
		return;

	// local storage directory
	auto path_storage = &self->storage->config->path;
	if (str_empty(path_storage))
	{
		fs_mkdir(0755, "%s", path);
		return;
	}

	// external tier storage directory
	char path_ext[PATH_MAX];
	sfmt(path_ext, PATH_MAX, "%s/%s", str_of(path_storage), id);

	// <storage_path>
	if (! fs_exists("%s", str_of(path_storage)))
		fs_mkdir(0755, "%s", str_of(path_storage));

	// <storage_path>/<id_tier_storage>
	if (! fs_exists("%s", path_ext))
		fs_mkdir(0755, "%s", path_ext);

	// create symlink to the tier storage

	// <base>/storage/<id_tier_storage> -> <storage_path>/<id_tier_storage>
	if (symlink(path_ext, path) == -1)
		error_system();
}

static inline void
tier_storage_rmdir(TierStorage* self)
{
	if (! self->storage)
		return;

	// <storage_path>/<id_tier_storage>
	char id[UUID_SZ];
	uuid_get(&self->id, id, sizeof(id));

	char path[PATH_MAX];
	sfmt(path, PATH_MAX, "%s/storage/%s", state_directory(), id);
	if (! fs_exists("%s", path))
		return;

	// local storage directory
	auto path_storage = &self->storage->config->path;
	if (str_empty(path_storage))
	{
		fs_rmdir(true, "%s", path);
		return;
	}

	// delete symlink

	// <base>/storage/<id_tier_storage>
	vfs_unlink(path);

	// delete external tier storage directory
	char path_ext[PATH_MAX];
	sfmt(path_ext, PATH_MAX, "%s/%s", str_of(path_storage), id);

	// <storage_path>/<id_tier_storage>
	if (fs_exists("%s", path_ext))
		fs_rmdir(true, "%s", path_ext);
}
