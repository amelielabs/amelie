#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct SchemaConfig SchemaConfig;

struct SchemaConfig
{
	Str  name;
	bool system;
};

static inline SchemaConfig*
schema_config_allocate(void)
{
	SchemaConfig* self;
	self = mn_malloc(sizeof(SchemaConfig));
	self->system = false;
	str_init(&self->name);
	return self;
}

static inline void
schema_config_free(SchemaConfig* self)
{
	str_free(&self->name);
	mn_free(self);
}

static inline void
schema_config_set_system(SchemaConfig* self, bool system)
{
	self->system = system;
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
	guard(copy_guard, schema_config_free, copy);
	schema_config_set_name(copy, &self->name);
	schema_config_set_system(copy, self->system);
	return unguard(&copy_guard);
}

static inline SchemaConfig*
schema_config_read(uint8_t** pos)
{
	auto self = schema_config_allocate();
	guard(self_guard, schema_config_free, self);

	// map
	int count;
	data_read_map(pos, &count);

	// name
	data_skip(pos);
	data_read_string_copy(pos, &self->name);

	// system
	data_skip(pos);
	data_read_bool(pos, &self->system);

	return unguard(&self_guard);
}

static inline void
schema_config_write(SchemaConfig* self, Buf* buf)
{
	// map
	encode_map(buf, 2);

	// name
	encode_raw(buf, "name", 4);
	encode_string(buf, &self->name);

	// system
	encode_raw(buf, "system", 6);
	encode_bool(buf, self->system);
}
