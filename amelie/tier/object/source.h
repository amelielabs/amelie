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

typedef struct Source Source;

struct Source
{
	Str     name;
	Str     path;
	bool    sync;
	bool    crc;
	int64_t refresh_wm;
	int64_t region_size;
	int64_t part_size;
	Str     compression;
	int64_t compression_level;
	Str     encryption;
	Str     encryption_key;
	bool    system;
};

static inline Source*
source_allocate(void)
{
	auto self = (Source*)am_malloc(sizeof(Source));
	self->sync              = true;
	self->crc               = false;
	self->compression_level = 0;
	self->region_size       = 64 * 1024;
	self->part_size         = 64 * 1024 * 1024;
	self->refresh_wm        = 40 * 1024 * 1024;
	self->system            = false;
	str_init(&self->name);
	str_init(&self->path);
	str_init(&self->compression);
	str_init(&self->encryption);
	str_init(&self->encryption_key);
	return self;
}

static inline void
source_free(Source* self)
{
	str_free(&self->name);
	str_free(&self->path);
	str_free(&self->compression);
	str_free(&self->encryption);
	str_free(&self->encryption_key);
	am_free(self);
}

static inline void
source_set_name(Source* self, Str* value)
{
	str_free(&self->name);
	str_copy(&self->name, value);
}

static inline void
source_set_path(Source* self, Str* value)
{
	str_free(&self->path);
	str_copy(&self->path, value);
}

static inline void
source_set_sync(Source* self, bool value)
{
	self->sync = value;
}

static inline void
source_set_crc(Source* self, bool value)
{
	self->crc = value;
}

static inline void
source_set_refresh_wm(Source* self, int value)
{
	self->refresh_wm = value;
}

static inline void
source_set_region_size(Source* self, int value)
{
	self->region_size = value;
}

static inline void
source_set_compression(Source* self, Str* value)
{
	str_free(&self->compression);
	str_copy(&self->compression, value);
}

static inline void
source_set_compression_level(Source* self, int value)
{
	self->compression_level = value;
}

static inline void
source_set_encryption(Source* self, Str* value)
{
	str_free(&self->encryption);
	str_copy(&self->encryption, value);
}

static inline void
source_set_encryption_key(Source* self, Str* value)
{
	str_free(&self->encryption_key);
	str_copy(&self->encryption_key, value);
}

static inline void
source_set_system(Source* self, bool value)
{
	self->system = value;
}

static inline Source*
source_copy(Source* self)
{
	auto copy = source_allocate();
	source_set_name(copy, &self->name);
	source_set_path(copy, &self->path);
	source_set_sync(copy, self->sync);
	source_set_crc(copy, self->crc);
	source_set_refresh_wm(copy, self->refresh_wm);
	source_set_region_size(copy, self->region_size);
	source_set_compression(copy, &self->compression);
	source_set_compression_level(copy, self->compression_level);
	source_set_encryption(copy, &self->encryption);
	source_set_encryption_key(copy, &self->encryption_key);
	source_set_system(copy, self->system);
	return copy;
}

static inline Source*
source_read(uint8_t** pos)
{
	auto self = source_allocate();
	errdefer(source_free, self);
	Decode obj[] =
	{
		{ DECODE_STRING, "name",              &self->name              },
		{ DECODE_STRING, "path",              &self->path              },
		{ DECODE_BOOL,   "sync",              &self->sync              },
		{ DECODE_BOOL,   "crc",               &self->crc               },
		{ DECODE_INT,    "refresh_wm",        &self->refresh_wm        },
		{ DECODE_INT,    "region_size",       &self->region_size       },
		{ DECODE_STRING, "compression",       &self->compression       },
		{ DECODE_INT,    "compression_level", &self->compression_level },
		{ DECODE_STRING, "encryption",        &self->encryption        },
		{ DECODE_STRING, "encryption_key",    &self->encryption_key    },
		{ 0,              NULL,               NULL                     },
	};
	decode_obj(obj, "source", pos);
	return self;
}

static inline void
source_write(Source* self, Buf* buf, bool safe)
{
	// map
	encode_obj(buf);

	// name
	encode_raw(buf, "name", 4);
	encode_string(buf, &self->name);

	// path
	encode_raw(buf, "path", 4);
	encode_string(buf, &self->path);

	// sync
	encode_raw(buf, "sync", 4);
	encode_bool(buf, self->sync);

	// crc
	encode_raw(buf, "crc", 3);
	encode_bool(buf, self->crc);

	// refresh_wm
	encode_raw(buf, "refresh_wm", 10);
	encode_integer(buf, self->refresh_wm);

	// region_size
	encode_raw(buf, "region_size", 11);
	encode_integer(buf, self->region_size);

	// compression
	encode_raw(buf, "compression", 11);
	encode_string(buf, &self->compression);

	// compression_level
	encode_raw(buf, "compression_level", 17);
	encode_integer(buf, self->compression_level);

	// encryption
	encode_raw(buf, "encryption", 10);
	encode_string(buf, &self->encryption);

	if (! safe)
	{
		// encryption_key
		encode_raw(buf, "encryption_key", 14);
		encode_string(buf, &self->encryption_key);
	}

	encode_obj_end(buf);
}

static inline void
source_basepath(Source* self, char* path)
{
	// set full e path
	if (str_empty(&self->path))
	{
		// <base>
		sfmt(path, PATH_MAX, "%s", state_directory());
	} else
	{
		if (*str_of(&self->path) == '/')
		{
			// /<path>
			sfmt(path, PATH_MAX, "%.*s", str_size(&self->path),
			     str_of(&self->path));
		} else
		{
			// <base>/<path>
			sfmt(path, PATH_MAX, "%s/%.*s", state_directory(),
			     str_size(&self->path),
			     str_of(&self->path));
		}
	}
}

static inline void format_validate(4, 5)
source_path(Source* self, char* path, Uuid* id, char* fmt, ...)
{
	// set relative path
	char relative[512];
	va_list args;
	va_start(args, fmt);
	vsfmt(relative, sizeof(relative), fmt, args);
	va_end(args);

	char uuid[UUID_SZ];
	uuid_get(id, uuid, sizeof(uuid));

	// set full storage path
	if (str_empty(&self->path))
	{
		// <base>/<table_uuid>/*
		sfmt(path, PATH_MAX, "%s/%s/%s", state_directory(), uuid, relative);
	} else
	{
		if (*str_of(&self->path) == '/')
		{
			// <absolute_path>/<table_uuid>/*
			sfmt(path, PATH_MAX, "%.*s/%s/%s", str_size(&self->path),
			     str_of(&self->path), uuid, relative);
		} else
		{
			// <base>/<table_uuid>/*
			sfmt(path, PATH_MAX, "%s/%.*s/%s/%s", state_directory(),
			     str_size(&self->path),
			     str_of(&self->path), uuid, relative);
		}
	}
}
