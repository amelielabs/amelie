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

typedef struct SchemaConfig SchemaConfig;

struct SchemaConfig
{
	Str  name;
	bool system;
	bool create;
};

static inline SchemaConfig*
schema_config_allocate(void)
{
	SchemaConfig* self;
	self = am_malloc(sizeof(SchemaConfig));
	self->system = false;
	self->create = false;
	str_init(&self->name);
	return self;
}

static inline void
schema_config_free(SchemaConfig* self)
{
	str_free(&self->name);
	am_free(self);
}

static inline void
schema_config_set_system(SchemaConfig* self, bool system)
{
	self->system = system;
}

static inline void
schema_config_set_create(SchemaConfig* self, bool create)
{
	self->create = create;
}

static inline void
schema_config_set_name(SchemaConfig* self, Str* name)
{
	str_free(&self->name);
	str_copy(&self->name, name);
}

static inline SchemaConfig*
schema_config_copy(SchemaConfig* self)
{
	auto copy = schema_config_allocate();
	schema_config_set_name(copy, &self->name);
	schema_config_set_system(copy, self->system);
	schema_config_set_create(copy, self->create);
	return copy;
}

static inline SchemaConfig*
schema_config_read(uint8_t** pos)
{
	auto self = schema_config_allocate();
	errdefer(schema_config_free, self);
	Decode obj[] =
	{
		{ DECODE_STRING, "name",       &self->name   },
		{ DECODE_BOOL,   "system",     &self->system },
		{ DECODE_BOOL,   "create",     &self->create },
		{ 0,              NULL,        NULL          },
	};
	decode_obj(obj, "schema", pos);
	return self;
}

static inline void
schema_config_write(SchemaConfig* self, Buf* buf)
{
	// obj
	encode_obj(buf);

	// name
	encode_raw(buf, "name", 4);
	encode_string(buf, &self->name);

	// system
	encode_raw(buf, "system", 6);
	encode_bool(buf, self->system);

	// create
	encode_raw(buf, "create", 6);
	encode_bool(buf, self->create);

	encode_obj_end(buf);
}

static inline void
schema_config_write_compact(SchemaConfig* self, Buf* buf)
{
	// obj
	encode_obj(buf);

	// name
	encode_raw(buf, "name", 4);
	encode_string(buf, &self->name);

	// system
	encode_raw(buf, "system", 6);
	encode_bool(buf, self->system);

	encode_obj_end(buf);
}
