#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct UdfConfig UdfConfig;

struct UdfConfig
{
	Str     schema;
	Str     name;
	Str     text;
	Columns columns;
};

static inline UdfConfig*
udf_config_allocate(void)
{
	UdfConfig* self;
	self = am_malloc(sizeof(UdfConfig));
	str_init(&self->schema);
	str_init(&self->name);
	str_init(&self->text);
	columns_init(&self->columns);
	return self;
}

static inline void
udf_config_free(UdfConfig* self)
{
	str_free(&self->schema);
	str_free(&self->name);
	str_free(&self->text);
	columns_free(&self->columns);
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

static inline UdfConfig*
udf_config_copy(UdfConfig* self)
{
	auto copy = udf_config_allocate();
	udf_config_set_schema(copy, &self->schema);
	udf_config_set_name(copy, &self->name);
	udf_config_set_text(copy, &self->text);
	columns_copy(&copy->columns, &self->columns);
	return copy;
}

static inline UdfConfig*
udf_config_read(uint8_t** pos)
{
	auto self = udf_config_allocate();
	guard(udf_config_free, self);
	uint8_t* columns = NULL;
	Decode map[] =
	{
		{ DECODE_STRING, "schema",  &self->schema },
		{ DECODE_STRING, "name",    &self->name   },
		{ DECODE_STRING, "text",    &self->text   },
		{ DECODE_ARRAY,  "columns", &columns      },
		{ 0,              NULL,      NULL         },
	};
	decode_map(map, "udf", pos);
	columns_read(&self->columns, &columns);
	return unguard();
}

static inline void
udf_config_write(UdfConfig* self, Buf* buf)
{
	// map
	encode_map(buf);

	// schema
	encode_raw(buf, "schema", 6);
	encode_string(buf, &self->schema);

	// name
	encode_raw(buf, "name", 4);
	encode_string(buf, &self->name);

	// text
	encode_raw(buf, "text", 4);
	encode_string(buf, &self->text);

	// columns
	encode_raw(buf, "columns", 7);
	columns_write(&self->columns, buf);

	encode_map_end(buf);
}
