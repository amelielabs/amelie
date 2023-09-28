#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct IndexConfig IndexConfig;

enum
{
	INDEX_UNDEF,
	INDEX_HASH,
	INDEX_TREE
};

struct IndexConfig
{
	Str     name;
	int64_t type;
	bool    primary;
	Schema  schema;
};

static inline IndexConfig*
index_config_allocate(void)
{
	IndexConfig *self;
	self = mn_malloc(sizeof(IndexConfig));
	self->type    = INDEX_UNDEF;
	self->primary = false;
	str_init(&self->name);
	schema_init(&self->schema);
	return self;
}

static inline void
index_config_free(IndexConfig* self)
{
	str_free(&self->name);
	schema_free(&self->schema);
	mn_free(self);
}

static inline void
index_config_set_name(IndexConfig* self, Str* name)
{
	str_copy(&self->name, name);
}

static inline void
index_config_set_primary(IndexConfig* self, bool primary)
{
	self->primary = primary;
}

static inline void
index_config_set_type(IndexConfig* self, int type)
{
	self->type = type;
}

static inline IndexConfig*
index_config_copy(IndexConfig* self)
{
	auto copy = index_config_allocate();
	guard(copy_guard, index_config_free, copy);
	index_config_set_name(copy, &self->name);
	index_config_set_type(copy, self->type);
	index_config_set_primary(copy, self->primary);
	schema_copy(&copy->schema, &self->schema);
	return unguard(&copy_guard);
}

static inline IndexConfig*
index_config_read(uint8_t** pos)
{
	auto self = index_config_allocate();
	guard(self_guard, index_config_free, self);

	// map
	int count;
	data_read_map(pos, &count);

	// name
	data_skip(pos);
	data_read_string_copy(pos, &self->name);

	// type
	data_skip(pos);
	data_read_integer(pos, &self->type);

	// primary
	data_skip(pos);
	data_read_bool(pos, &self->primary);

	// schema
	data_skip(pos);
	schema_read(&self->schema, pos);
	return unguard(&self_guard);
}

static inline void
index_config_write(IndexConfig* self, Buf* buf)
{
	// map
	encode_map(buf, 4);

	// name
	encode_raw(buf, "name", 4);
	encode_string(buf, &self->name);

	// type
	encode_raw(buf, "type", 4);
	encode_integer(buf, self->type);

	// primary
	encode_raw(buf, "primary", 7);
	encode_bool(buf, self->primary);

	// schema
	encode_raw(buf, "schema", 6);
	schema_write(&self->schema, buf);
}
