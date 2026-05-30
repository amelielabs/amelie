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

typedef struct Storage Storage;

struct Storage
{
	Uuid id;
	Str  compression;
	int  compression_level;
};

static inline void
storage_init(Storage* self)
{
	self->compression_level = 0;
	str_init(&self->compression);
	uuid_init(&self->id);
}

static inline void
storage_free(Storage* self)
{
	str_free(&self->compression);
}

static inline void
storage_set_id(Storage* self, Uuid* id)
{
	self->id = *id;
}

static inline void
storage_set_compression(Storage* self, Str* value)
{
	str_free(&self->compression);
	str_copy(&self->compression, value);
}

static inline void
storage_set_compression_level(Storage* self, int value)
{
	self->compression_level = value;
}

static inline void
storage_copy(Storage* self, Storage* from)
{
	storage_set_id(self, &from->id);
	storage_set_compression(self, &from->compression);
	storage_set_compression_level(self, from->compression_level);
}

static inline void
storage_read(Storage* self, uint8_t** pos)
{
	Decode obj[] =
	{
		{ DECODE_STR,  "compression",       &self->compression       },
		{ DECODE_INT,  "compression_level", &self->compression_level },
		{ 0,            NULL,               NULL                     },
	};
	decode_obj(obj, "storage", pos);
}

static inline void
storage_write(Storage* self, Buf* buf, int flags)
{
	unused(flags);

	// map
	encode_obj(buf);

	// compression
	encode_raw(buf, "compression", 11);
	encode_str(buf, &self->compression);

	// compression_level
	encode_raw(buf, "compression_level", 17);
	encode_int(buf, self->compression_level);

	encode_obj_end(buf);
}

static inline void
storage_mkdir(Storage* self)
{
	// <base>/storage/<storage_id>
	char id[UUID_SZ];
	uuid_get(&self->id, id, sizeof(id));

	char path[PATH_MAX];
	format(path, PATH_MAX, "{s}/storage/{s}", state_directory(), id);
	if (fs_exists("{s}", path))
		return;

	fs_mkdir(0755, "{s}", path);
}

static inline void
storage_rmdir(Storage* self)
{
	// <base>/storage/<storage_id>
	char id[UUID_SZ];
	uuid_get(&self->id, id, sizeof(id));

	char path[PATH_MAX];
	format(path, PATH_MAX, "{s}/storage/{s}", state_directory(), id);
	if (! fs_exists("{s}", path))
		return;

	// must be empty at this point
	fs_rmdir(true, "{s}", path);
}
