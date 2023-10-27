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
	Str     schema;
	Str     name;
	Str     data;
	Key     key;
};

static inline MetaConfig*
meta_config_allocate(void)
{
	MetaConfig* self;
	self = mn_malloc(sizeof(MetaConfig));
	self->type = META_UNDEF;
	str_init(&self->schema);
	str_init(&self->name);
	str_init(&self->data);
	key_init(&self->key);
	return self;
}

static inline void
meta_config_free(MetaConfig* self)
{
	str_free(&self->schema);
	str_free(&self->name);
	str_free(&self->data);
	key_free(&self->key);
	mn_free(self);
}

static inline void
meta_config_set_type(MetaConfig* self, MetaType type)
{
	self->type = type;
}

static inline void
meta_config_set_schema(MetaConfig* self, Str* schema)
{
	str_copy(&self->schema, schema);
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
	meta_config_set_schema(copy, &self->schema);
	meta_config_set_name(copy, &self->name);
	meta_config_set_data(copy, &self->data);
	key_copy(&copy->key, &self->key);
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

	// schema
	data_skip(pos);
	data_read_string_copy(pos, &self->schema);

	// name
	data_skip(pos);
	data_read_string_copy(pos, &self->name);

	// key
	data_skip(pos);
	key_read(&self->key, pos);

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
	encode_map(buf, 5);

	// type
	encode_raw(buf, "type", 4);
	encode_integer(buf, self->type);

	// schema
	encode_raw(buf, "schema", 6);
	encode_string(buf, &self->schema);

	// name
	encode_raw(buf, "name", 4);
	encode_string(buf, &self->name);

	// key
	encode_raw(buf, "key", 3);
	key_write(&self->key, buf);

	// data
	encode_raw(buf, "data", 4);
	buf_write_str(buf, &self->data);
}
