#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct TableConfig TableConfig;

struct TableConfig
{
	Uuid   id;
	Str    name;
	Schema schema;
};

static inline TableConfig*
table_config_allocate(void)
{
	TableConfig* self;
	self = mn_malloc(sizeof(TableConfig));
	uuid_init(&self->id);
	str_init(&self->name);
	schema_init(&self->schema);
	return self;
}

static inline void
table_config_free(TableConfig* self)
{
	str_free(&self->name);
	schema_free(&self->schema);
	mn_free(self);
}

static inline void
table_config_set_id(TableConfig* self, Uuid* id)
{
	self->id = *id;
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
	table_config_set_name(copy, &self->name);
	schema_copy(&copy->schema, &self->schema);
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

	// name
	data_skip(pos);
	data_read_string_copy(pos, &self->name);

	// schema
	data_skip(pos);
	schema_read(&self->schema, pos);

	return unguard(&self_guard);
}

static inline void
table_config_write(TableConfig* self, Buf* buf)
{
	// map
	encode_map(buf, 3);

	// id
	encode_raw(buf, "id", 2);
	char uuid[UUID_SZ];
	uuid_to_string(&self->id, uuid, sizeof(uuid));
	encode_raw(buf, uuid, sizeof(uuid) - 1);

	// name
	encode_raw(buf, "name", 4);
	encode_string(buf, &self->name);

	// schema
	encode_raw(buf, "schema", 6);
	schema_write(&self->schema, buf);
}
