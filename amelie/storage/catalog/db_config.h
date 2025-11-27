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

typedef struct DbConfig DbConfig;

struct DbConfig
{
	Str  name;
	bool main;
};

static inline DbConfig*
db_config_allocate(void)
{
	DbConfig* self;
	self = am_malloc(sizeof(DbConfig));
	self->main = false;
	str_init(&self->name);
	return self;
}

static inline void
db_config_free(DbConfig* self)
{
	str_free(&self->name);
	am_free(self);
}

static inline void
db_config_set_main(DbConfig* self, bool value)
{
	self->main = value;
}

static inline void
db_config_set_name(DbConfig* self, Str* name)
{
	str_free(&self->name);
	str_copy(&self->name, name);
}

static inline DbConfig*
db_config_copy(DbConfig* self)
{
	auto copy = db_config_allocate();
	db_config_set_name(copy, &self->name);
	db_config_set_main(copy, self->main);
	return copy;
}

static inline DbConfig*
db_config_read(uint8_t** pos)
{
	auto self = db_config_allocate();
	errdefer(db_config_free, self);
	Decode obj[] =
	{
		{ DECODE_STRING, "name", &self->name },
		{ DECODE_BOOL,   "main", &self->main },
		{ 0,              NULL,   NULL       },
	};
	decode_obj(obj, "db", pos);
	return self;
}

static inline void
db_config_write(DbConfig* self, Buf* buf)
{
	// obj
	encode_obj(buf);

	// name
	encode_raw(buf, "name", 4);
	encode_string(buf, &self->name);

	// main
	encode_raw(buf, "main", 4);
	encode_bool(buf, self->main);

	encode_obj_end(buf);
}

static inline void
db_config_write_compact(DbConfig* self, Buf* buf)
{
	// obj
	encode_obj(buf);

	// name
	encode_raw(buf, "name", 4);
	encode_string(buf, &self->name);

	// main
	encode_raw(buf, "main", 4);
	encode_bool(buf, self->main);

	encode_obj_end(buf);
}
