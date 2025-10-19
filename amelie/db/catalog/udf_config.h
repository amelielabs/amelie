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
	Str     schema;
	Str     name;
	Str     text;
	Columns args;
	int64_t type;
};

static inline UdfConfig*
udf_config_allocate(void)
{
	UdfConfig* self;
	self = am_malloc(sizeof(UdfConfig));
	self->type = TYPE_NULL;
	str_init(&self->schema);
	str_init(&self->name);
	str_init(&self->text);
	columns_init(&self->args);
	return self;
}

static inline void
udf_config_free(UdfConfig* self)
{
	str_free(&self->schema);
	str_free(&self->name);
	str_free(&self->text);
	columns_free(&self->args);
	am_free(self);
}

static inline void
udf_config_set_schema(UdfConfig* self, Str* schema)
{
	str_free(&self->schema);
	str_copy(&self->schema, schema);
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
	udf_config_set_schema(copy, &self->schema);
	udf_config_set_name(copy, &self->name);
	udf_config_set_text(copy, &self->text);
	udf_config_set_type(copy, self->type);
	columns_copy(&copy->args, &self->args);
	return copy;
}

static inline UdfConfig*
udf_config_read(uint8_t** pos)
{
	auto self = udf_config_allocate();
	errdefer(udf_config_free, self);
	uint8_t* args = NULL;
	Decode obj[] =
	{
		{ DECODE_STRING, "schema", &self->schema },
		{ DECODE_STRING, "name",   &self->name   },
		{ DECODE_STRING, "text",   &self->text   },
		{ DECODE_ARRAY,  "args",   &args         },
		{ DECODE_INT,    "type",   &self->type   },
		{ 0,              NULL,     NULL         },
	};
	decode_obj(obj, "udf", pos);
	columns_read(&self->args, &args);
	return self;
}

static inline void
udf_config_write(UdfConfig* self, Buf* buf)
{
	// map
	encode_obj(buf);

	// schema
	encode_raw(buf, "schema", 6);
	encode_string(buf, &self->schema);

	// name
	encode_raw(buf, "name", 4);
	encode_string(buf, &self->name);

	// text
	encode_raw(buf, "text", 4);
	encode_string(buf, &self->text);

	// type
	encode_raw(buf, "type", 4);
	encode_integer(buf, self->type);

	// args
	encode_raw(buf, "args", 4);
	columns_write(&self->args, buf);

	encode_obj_end(buf);
}

static inline void
udf_config_write_compact(UdfConfig* self, Buf* buf)
{
	// map
	encode_obj(buf);

	// schema
	encode_raw(buf, "schema", 6);
	encode_string(buf, &self->schema);

	// name
	encode_raw(buf, "name", 4);
	encode_string(buf, &self->name);

	encode_obj_end(buf);
}
