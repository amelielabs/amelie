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

typedef struct Volume Volume;

struct Volume
{
	Str      name;
	Uuid     id;
	Storage* storage;
	bool     pause;
	List     list;
	int      list_count;
	List     link;
};

static inline Volume*
volume_allocate(void)
{
	auto self = (Volume*)am_malloc(sizeof(Volume));
	self->storage = NULL;
	self->pause   = false;
	self->list_count = 0;
	list_init(&self->list);
	uuid_init(&self->id);
	str_init(&self->name);
	list_init(&self->link);
	return self;
}

static inline void
volume_free(Volume* self)
{
	assert(! self->list_count);
	str_free(&self->name);
	am_free(self);
}

static inline void
volume_add(Volume* self, List* link)
{
	list_append(&self->list, link);
	self->list_count++;
}

static inline void
volume_remove(Volume* self, List* link)
{
	list_unlink(link);
	self->list_count--;
	assert(self->list_count >= 0);
}

static inline void
volume_set_name(Volume* self, Str* name)
{
	str_free(&self->name);
	str_copy(&self->name, name);
}

static inline void
volume_set_id(Volume* self, Uuid* id)
{
	self->id = *id;
}

static inline void
volume_set_pause(Volume* self, bool value)
{
	self->pause = value;
}

static inline Volume*
volume_copy(Volume* from)
{
	auto self = volume_allocate();
	volume_set_name(self, &from->name);
	volume_set_id(self, &from->id);
	volume_set_pause(self, from->pause);
	return self;
}

static inline Volume*
volume_read(uint8_t** pos)
{
	auto self = volume_allocate();
	errdefer(volume_free, self);
	Decode obj[] =
	{
		{ DECODE_STRING, "name",  &self->name  },
		{ DECODE_UUID,   "id",    &self->id    },
		{ DECODE_BOOL,   "pause", &self->pause },
		{ 0,              NULL,    NULL        },
	};
	decode_obj(obj, "volume", pos);
	return self;
}

static inline void
volume_write(Volume* self, Buf* buf, int flags)
{
	unused(flags);

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

	encode_obj_end(buf);
}

static inline void
volume_mkdir(Volume* self)
{
	if (! self->storage)
		return;

	// <base>/storage/<volume_id>
	char id[UUID_SZ];
	uuid_get(&self->id, id, sizeof(id));

	char path[PATH_MAX];
	sfmt(path, PATH_MAX, "%s/storage/%s", state_directory(), id);
	if (fs_exists("%s", path))
		return;

	// local directory
	auto path_storage = &self->storage->config->path;
	if (str_empty(path_storage))
	{
		fs_mkdir(0755, "%s", path);
		return;
	}

	// external storage directory
	char path_ext[PATH_MAX];
	sfmt(path_ext, PATH_MAX, "%s/%s", str_of(path_storage), id);

	// <storage_path>
	if (! fs_exists("%s", str_of(path_storage)))
		fs_mkdir(0755, "%s", str_of(path_storage));

	// <storage_path>/<volume_id>
	if (! fs_exists("%s", path_ext))
		fs_mkdir(0755, "%s", path_ext);

	// create symlink to the directory

	// <base>/storage/<volume_id> -> <storage_path>/<volume_id>
	if (symlink(path_ext, path) == -1)
		error_system();
}

static inline void
volume_rmdir(Volume* self)
{
	if (! self->storage)
		return;

	// <storage_path>/<volume_id>
	char id[UUID_SZ];
	uuid_get(&self->id, id, sizeof(id));

	char path[PATH_MAX];
	sfmt(path, PATH_MAX, "%s/storage/%s", state_directory(), id);
	if (! fs_exists("%s", path))
		return;

	// local directory
	auto path_storage = &self->storage->config->path;
	if (str_empty(path_storage))
	{
		fs_rmdir(true, "%s", path);
		return;
	}

	// delete symlink

	// <base>/storage/<volume_id>
	vfs_unlink(path);

	// delete external directory
	char path_ext[PATH_MAX];
	sfmt(path_ext, PATH_MAX, "%s/%s", str_of(path_storage), id);

	// <storage_path>/<volume_id>
	if (fs_exists("%s", path_ext))
		fs_rmdir(true, "%s", path_ext);
}
