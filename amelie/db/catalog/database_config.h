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

typedef struct DatabaseConfig DatabaseConfig;

struct DatabaseConfig
{
	Str  name;
	bool system;
};

static inline DatabaseConfig*
database_config_allocate()
{
	DatabaseConfig* self;
	self = am_malloc(sizeof(DatabaseConfig));
	self->system = false;
	str_init(&self->name);
	return self;
}

static inline void
database_config_free(DatabaseConfig* self)
{
	str_free(&self->name);
	am_free(self);
}

static inline void
database_config_set_system(DatabaseConfig* self, bool system)
{
	self->system = system;
}

static inline void
database_config_set_name(DatabaseConfig* self, Str* name)
{
	str_free(&self->name);
	str_copy(&self->name, name);
}

static inline DatabaseConfig*
database_config_copy(DatabaseConfig* self)
{
	auto copy = database_config_allocate();
	database_config_set_name(copy, &self->name);
	database_config_set_system(copy, self->system);
	return copy;
}

static inline DatabaseConfig*
database_config_read(uint8_t** pos)
{
	auto self = database_config_allocate();
	errdefer(database_config_free, self);
	Decode obj[] =
	{
		{ DECODE_STRING, "name", &self->name },
		{ 0,              NULL,   NULL       },
	};
	decode_obj(obj, "db", pos);
	return self;
}

static inline void
database_config_write(DatabaseConfig* self, Buf* buf)
{
	// obj
	encode_obj(buf);

	// name
	encode_raw(buf, "name", 4);
	encode_string(buf, &self->name);

	encode_obj_end(buf);
}

static inline void
database_config_write_compact(DatabaseConfig* self, Buf* buf)
{
	// obj
	encode_obj(buf);

	// name
	encode_raw(buf, "name", 4);
	encode_string(buf, &self->name);

	encode_obj_end(buf);
}
