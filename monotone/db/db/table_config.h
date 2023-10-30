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
	bool reference;
	Def  def;
};

static inline TableConfig*
table_config_allocate(void)
{
	TableConfig* self;
	self = mn_malloc(sizeof(TableConfig));
	self->reference = false;
	uuid_init(&self->id);
	str_init(&self->schema);
	str_init(&self->name);
	def_init(&self->def);
	return self;
}

static inline void
table_config_free(TableConfig* self)
{
	str_free(&self->schema);
	str_free(&self->name);
	def_free(&self->def);
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

static inline void
table_config_set_reference(TableConfig* self, bool reference)
{
	self->reference = reference;
}

static inline TableConfig*
table_config_copy(TableConfig* self)
{
	auto copy = table_config_allocate();
	guard(copy_guard, table_config_free, copy);
	table_config_set_id(copy, &self->id);
	table_config_set_schema(copy, &self->schema);
	table_config_set_name(copy, &self->name);
	table_config_set_reference(copy, self->reference);
	def_copy(&copy->def, &self->def);
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

	// reference
	data_skip(pos);
	data_read_bool(pos, &self->reference);

	// def
	data_skip(pos);
	def_read(&self->def, pos);

	return unguard(&self_guard);
}

static inline void
table_config_write(TableConfig* self, Buf* buf)
{
	// map
	encode_map(buf, 5);

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

	// reference
	encode_raw(buf, "reference", 9);
	encode_bool(buf, self->reference);

	// def
	encode_raw(buf, "def", 3);
	def_write(&self->def, buf);
}
