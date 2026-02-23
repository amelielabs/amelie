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

typedef struct StorageConfig StorageConfig;

struct StorageConfig
{
	Str     name;
	Str     path;
	Str     compression;
	int64_t compression_level;
	bool    system;
};

static inline StorageConfig*
storage_config_allocate(void)
{
	auto self = (StorageConfig*)am_malloc(sizeof(StorageConfig));
	self->compression_level = 0;
	self->system = false;
	str_init(&self->compression);
	str_init(&self->name);
	str_init(&self->path);
	return self;
}

static inline void
storage_config_free(StorageConfig* self)
{
	str_free(&self->name);
	str_free(&self->path);
	am_free(self);
}

static inline void
storage_config_set_name(StorageConfig* self, Str* value)
{
	str_free(&self->name);
	str_copy(&self->name, value);
}

static inline void
storage_config_set_path(StorageConfig* self, Str* value)
{
	str_free(&self->path);
	str_copy(&self->path, value);
}

static inline void
storage_config_set_compression(StorageConfig* self, Str* value)
{
	str_free(&self->compression);
	str_copy(&self->compression, value);
}

static inline void
storage_config_set_compression_level(StorageConfig* self, int value)
{
	self->compression_level = value;
}

static inline void
storage_config_set_system(StorageConfig* self, bool value)
{
	self->system = value;
}

static inline StorageConfig*
storage_config_copy(StorageConfig* self)
{
	auto copy = storage_config_allocate();
	storage_config_set_name(copy, &self->name);
	storage_config_set_path(copy, &self->path);
	storage_config_set_compression(copy, &self->compression);
	storage_config_set_compression_level(copy, self->compression_level);
	storage_config_set_system(copy, self->system);
	return copy;
}

static inline StorageConfig*
storage_config_read(uint8_t** pos)
{
	auto self = storage_config_allocate();
	errdefer(storage_config_free, self);
	Decode obj[] =
	{
		{ DECODE_STRING, "name",              &self->name              },
		{ DECODE_STRING, "path",              &self->path              },
		{ DECODE_STRING, "compression",       &self->compression       },
		{ DECODE_INT,    "compression_level", &self->compression_level },
		{ 0,              NULL,                NULL                    },
	};
	decode_obj(obj, "storage_config", pos);
	return self;
}

static inline void
storage_config_write(StorageConfig* self, Buf* buf, int flags)
{
	unused(flags);

	// map
	encode_obj(buf);

	// name
	encode_raw(buf, "name", 4);
	encode_string(buf, &self->name);

	// path
	encode_raw(buf, "path", 4);
	encode_string(buf, &self->path);

	// compression
	encode_raw(buf, "compression", 11);
	encode_string(buf, &self->compression);

	// compression_level
	encode_raw(buf, "compression_level", 17);
	encode_integer(buf, self->compression_level);

	encode_obj_end(buf);
}
