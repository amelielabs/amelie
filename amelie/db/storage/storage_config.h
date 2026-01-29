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
	Str  name;
	Str  path;
	bool sync;
	bool system;
};

static inline StorageConfig*
storage_config_allocate(void)
{
	auto self = (StorageConfig*)am_malloc(sizeof(StorageConfig));
	self->sync   = true;
	self->system = false;
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
storage_config_set_sync(StorageConfig* self, bool value)
{
	self->sync = value;
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
	storage_config_set_sync(copy, self->sync);
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
		{ DECODE_STRING, "name", &self->name },
		{ DECODE_STRING, "path", &self->path },
		{ DECODE_BOOL,   "sync", &self->sync },
		{ 0,              NULL,   NULL       },
	};
	decode_obj(obj, "storage_config", pos);
	return self;
}

static inline void
storage_config_write(StorageConfig* self, Buf* buf)
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

	encode_obj_end(buf);
}
