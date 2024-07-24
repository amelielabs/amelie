#pragma once

//
// sonata.
//
// Real-Time SQL Database.
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
	self = so_malloc(sizeof(SchemaConfig));
	self->system = false;
	self->create = false;
	str_init(&self->name);
	return self;
}

static inline void
schema_config_free(SchemaConfig* self)
{
	str_free(&self->name);
	so_free(self);
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
	guard(schema_config_free, copy);
	schema_config_set_name(copy, &self->name);
	schema_config_set_system(copy, self->system);
	schema_config_set_create(copy, self->create);
	return unguard();
}

static inline SchemaConfig*
schema_config_read(uint8_t** pos)
{
	auto self = schema_config_allocate();
	guard(schema_config_free, self);
	Decode map[] =
	{
		{ DECODE_STRING, "name",       &self->name   },
		{ DECODE_BOOL,   "system",     &self->system },
		{ DECODE_BOOL,   "create",     &self->create },
		{ 0,              NULL,        NULL          },
	};
	decode_map(map, pos);
	return unguard();
}

static inline void
schema_config_write(SchemaConfig* self, Buf* buf)
{
	// map
	encode_map(buf);

	// name
	encode_raw(buf, "name", 4);
	encode_string(buf, &self->name);

	// system
	encode_raw(buf, "system", 6);
	encode_bool(buf, self->system);

	// create
	encode_raw(buf, "create", 6);
	encode_bool(buf, self->create);

	encode_map_end(buf);
}
