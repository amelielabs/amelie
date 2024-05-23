#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct ViewConfig ViewConfig;

struct ViewConfig
{
	Str schema;
	Str name;
	Str query;
	Def def;
};

static inline ViewConfig*
view_config_allocate(void)
{
	ViewConfig* self;
	self = so_malloc(sizeof(ViewConfig));
	str_init(&self->schema);
	str_init(&self->name);
	str_init(&self->query);
	def_init(&self->def);
	return self;
}

static inline void
view_config_free(ViewConfig* self)
{
	str_free(&self->schema);
	str_free(&self->name);
	str_free(&self->query);
	def_free(&self->def);
	so_free(self);
}

static inline void
view_config_set_schema(ViewConfig* self, Str* schema)
{
	str_free(&self->schema);
	str_copy(&self->schema, schema);
}

static inline void
view_config_set_name(ViewConfig* self, Str* name)
{
	str_free(&self->name);
	str_copy(&self->name, name);
}

static inline void
view_config_set_query(ViewConfig* self, Str* query)
{
	str_copy(&self->query, query);
}

static inline ViewConfig*
view_config_copy(ViewConfig* self)
{
	auto copy = view_config_allocate();
	guard(view_config_free, copy);
	view_config_set_schema(copy, &self->schema);
	view_config_set_name(copy, &self->name);
	view_config_set_query(copy, &self->query);
	def_copy(&copy->def, &self->def);
	return unguard();
}

static inline ViewConfig*
view_config_read(uint8_t** pos)
{
	auto self = view_config_allocate();
	guard(view_config_free, self);

	uint8_t* def = NULL;
	Decode map[] =
	{
		{ DECODE_STRING, "schema", &self->schema },
		{ DECODE_STRING, "name",   &self->name   },
		{ DECODE_STRING, "query",  &self->query  },
		{ DECODE_MAP,    "def",    &def          },
		{ 0,             NULL,     NULL          },
	};
	decode_map(map, pos);
	def_read(&self->def, &def);

	return unguard();
}

static inline void
view_config_write(ViewConfig* self, Buf* buf)
{
	// map
	encode_map(buf, 4);

	// schema
	encode_raw(buf, "schema", 6);
	encode_string(buf, &self->schema);

	// name
	encode_raw(buf, "name", 4);
	encode_string(buf, &self->name);

	// query
	encode_raw(buf, "query", 5);
	encode_string(buf, &self->query);

	// def
	encode_raw(buf, "def", 3);
	def_write(&self->def, buf);
}
