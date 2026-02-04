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

typedef struct UdfConfig UdfConfig;

struct UdfConfig
{
	Str     db;
	Str     name;
	Str     text;
	Columns args;
	int64_t type;
	Columns returning;
};

static inline UdfConfig*
udf_config_allocate(void)
{
	UdfConfig* self;
	self = am_malloc(sizeof(UdfConfig));
	self->type = TYPE_NULL;
	str_init(&self->db);
	str_init(&self->name);
	str_init(&self->text);
	columns_init(&self->args);
	columns_init(&self->returning);
	return self;
}

static inline void
udf_config_free(UdfConfig* self)
{
	str_free(&self->db);
	str_free(&self->name);
	str_free(&self->text);
	columns_free(&self->args);
	columns_free(&self->returning);
	am_free(self);
}

static inline void
udf_config_set_db(UdfConfig* self, Str* db)
{
	str_free(&self->db);
	str_copy(&self->db, db);
}

static inline void
udf_config_set_name(UdfConfig* self, Str* name)
{
	str_free(&self->name);
	str_copy(&self->name, name);
}

static inline void
udf_config_set_text(UdfConfig* self, Str* text)
{
	str_copy(&self->text, text);
}

static inline void
udf_config_set_type(UdfConfig* self, Type type)
{
	self->type = type;
}

static inline UdfConfig*
udf_config_copy(UdfConfig* self)
{
	auto copy = udf_config_allocate();
	udf_config_set_db(copy, &self->db);
	udf_config_set_name(copy, &self->name);
	udf_config_set_text(copy, &self->text);
	udf_config_set_type(copy, self->type);
	columns_copy(&copy->args, &self->args);
	columns_copy(&copy->returning, &self->returning);
	return copy;
}

static inline UdfConfig*
udf_config_read(uint8_t** pos)
{
	auto self = udf_config_allocate();
	errdefer(udf_config_free, self);
	uint8_t* args = NULL;
	uint8_t* returning = NULL;
	Decode obj[] =
	{
		{ DECODE_STRING, "db",        &self->db   },
		{ DECODE_STRING, "name",      &self->name },
		{ DECODE_STRING, "text",      &self->text },
		{ DECODE_ARRAY,  "args",      &args       },
		{ DECODE_INT,    "type",      &self->type },
		{ DECODE_ARRAY,  "returning", &returning  },
		{ 0,              NULL,        NULL       },
	};
	decode_obj(obj, "udf", pos);
	columns_read(&self->args, &args);
	columns_read(&self->returning, &returning);
	return self;
}

static inline void
udf_config_write(UdfConfig* self, Buf* buf, int flags)
{
	// map
	encode_obj(buf);

	// db
	encode_raw(buf, "db", 2);
	encode_string(buf, &self->db);

	// name
	encode_raw(buf, "name", 4);
	encode_string(buf, &self->name);

	if (flags_has(flags, FMINIMAL))
	{
		encode_obj_end(buf);
		return;
	}

	// text
	encode_raw(buf, "text", 4);
	encode_string(buf, &self->text);

	// type
	encode_raw(buf, "type", 4);
	encode_integer(buf, self->type);

	// args
	encode_raw(buf, "args", 4);
	columns_write(&self->args, buf, flags);

	// args
	encode_raw(buf, "returning", 9);
	columns_write(&self->returning, buf, flags);

	encode_obj_end(buf);
}
