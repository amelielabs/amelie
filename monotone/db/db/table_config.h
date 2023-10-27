#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct TableConfig TableConfig;

struct TableConfig
{
	Uuid id;
	Str  schema;
	Str  name;
	Key  key;
};

static inline TableConfig*
table_config_allocate(void)
{
	TableConfig* self;
	self = mn_malloc(sizeof(TableConfig));
	uuid_init(&self->id);
	str_init(&self->schema);
	str_init(&self->name);
	key_init(&self->key);
	return self;
}

static inline void
table_config_free(TableConfig* self)
{
	str_free(&self->schema);
	str_free(&self->name);
	key_free(&self->key);
	mn_free(self);
}

static inline void
table_config_set_id(TableConfig* self, Uuid* id)
{
	self->id = *id;
}

static inline void
table_config_set_schema(TableConfig* self, Str* schema)
{
	str_copy(&self->schema, schema);
}

static inline void
table_config_set_name(TableConfig* self, Str* name)
{
	str_copy(&self->name, name);
}

static inline TableConfig*
table_config_copy(TableConfig* self)
{
	auto copy = table_config_allocate();
	guard(copy_guard, table_config_free, copy);
	table_config_set_id(copy, &self->id);
	table_config_set_schema(copy, &self->schema);
	table_config_set_name(copy, &self->name);
	key_copy(&copy->key, &self->key);
	return unguard(&copy_guard);
}

static inline TableConfig*
table_config_read(uint8_t** pos)
{
	auto self = table_config_allocate();
	guard(self_guard, table_config_free, self);

	// map
	int count;
	data_read_map(pos, &count);

	// id
	data_skip(pos);
	Str id;
	data_read_string(pos, &id);
	uuid_from_string(&self->id, &id);

	// schema
	data_skip(pos);
	data_read_string_copy(pos, &self->schema);

	// name
	data_skip(pos);
	data_read_string_copy(pos, &self->name);

	// key
	data_skip(pos);
	key_read(&self->key, pos);

	return unguard(&self_guard);
}

static inline void
table_config_write(TableConfig* self, Buf* buf)
{
	// map
	encode_map(buf, 4);

	// id
	encode_raw(buf, "id", 2);
	char uuid[UUID_SZ];
	uuid_to_string(&self->id, uuid, sizeof(uuid));
	encode_raw(buf, uuid, sizeof(uuid) - 1);

	// schema
	encode_raw(buf, "schema", 6);
	encode_string(buf, &self->schema);

	// name
	encode_raw(buf, "name", 4);
	encode_string(buf, &self->name);

	// key
	encode_raw(buf, "key", 3);
	key_write(&self->key, buf);
}
