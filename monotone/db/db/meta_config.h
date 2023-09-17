#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct MetaConfig MetaConfig;

typedef enum
{
	META_UNDEF,
	META_VIEW
} MetaType;

struct MetaConfig
{
	int64_t type;
	Str     name;
	Str     data;
	Schema  schema;
};

static inline MetaConfig*
meta_config_allocate(void)
{
	MetaConfig* self;
	self = mn_malloc(sizeof(MetaConfig));
	self->type = META_UNDEF;
	str_init(&self->name);
	str_init(&self->data);
	schema_init(&self->schema);
	return self;
}

static inline void
meta_config_free(MetaConfig* self)
{
	str_free(&self->name);
	str_free(&self->data);
	schema_free(&self->schema);
	mn_free(self);
}

static inline void
meta_config_set_type(MetaConfig* self, MetaType type)
{
	self->type = type;
}

static inline void
meta_config_set_name(MetaConfig* self, Str* name)
{
	str_copy(&self->name, name);
}

static inline void
meta_config_set_data(MetaConfig* self, Str* data)
{
	str_copy(&self->data, data);
}

static inline MetaConfig*
meta_config_copy(MetaConfig* self)
{
	auto copy = meta_config_allocate();
	guard(copy_guard, meta_config_free, copy);
	meta_config_set_type(copy, self->type);
	meta_config_set_name(copy, &self->name);
	meta_config_set_data(copy, &self->data);
	schema_copy(&copy->schema, &self->schema);
	return unguard(&copy_guard);
}

static inline MetaConfig*
meta_config_read(uint8_t** pos)
{
	auto self = meta_config_allocate();
	guard(self_guard, meta_config_free, self);

	// map
	int count;
	data_read_map(pos, &count);

	// type
	data_skip(pos);
	data_read_integer(pos, &self->type);

	// name
	data_skip(pos);
	data_read_string_copy(pos, &self->name);

	// schema
	data_skip(pos);
	schema_read(&self->schema, pos);

	// data
	data_skip(pos);
	uint8_t* data = *pos;
	data_skip(pos);
	str_strndup(&self->data, data, *pos - data);

	return unguard(&self_guard);
}

static inline void
meta_config_write(MetaConfig* self, Buf* buf)
{
	// map
	encode_map(buf, 4);

	// type
	encode_raw(buf, "type", 4);
	encode_integer(buf, self->type);

	// name
	encode_raw(buf, "name", 4);
	encode_string(buf, &self->name);

	// schema
	encode_raw(buf, "schema", 6);
	schema_write(&self->schema, buf);

	// data
	encode_raw(buf, "data", 4);
	buf_write_str(buf, &self->data);
}
