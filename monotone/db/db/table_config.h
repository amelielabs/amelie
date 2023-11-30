#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct TableConfig TableConfig;

struct TableConfig
{
	Uuid     id;
	Str      schema;
	Str      name;
	bool     reference;
	List     indexes;
	int      indexes_count;
	Mapping* map;
	List     storages;
	int      storages_count;
};

static inline TableConfig*
table_config_allocate(void)
{
	TableConfig* self;
	self = mn_malloc(sizeof(TableConfig));
	self->reference      = false;
	self->map            = NULL;
	self->indexes_count  = 0;
	self->storages_count = 0;
	uuid_init(&self->id);
	str_init(&self->schema);
	str_init(&self->name);
	list_init(&self->indexes);
	list_init(&self->storages);
	return self;
}

static inline void
table_config_free(TableConfig* self)
{
	str_free(&self->schema);
	str_free(&self->name);

	if (self->map)
		mapping_free(self->map);

	list_foreach_safe(&self->indexes)
	{
		auto config = list_at(IndexConfig, link);
		index_config_free(config);
	}

	list_foreach_safe(&self->storages)
	{
		auto config = list_at(StorageConfig, link);
		storage_config_free(config);
	}

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
	str_free(&self->schema);
	str_copy(&self->schema, schema);
}

static inline void
table_config_set_name(TableConfig* self, Str* name)
{
	str_free(&self->name);
	str_copy(&self->name, name);
}

static inline void
table_config_set_reference(TableConfig* self, bool reference)
{
	self->reference = reference;
}

static inline void
table_config_set_map(TableConfig* self, Mapping* map)
{
	self->map = map;
}

static inline void
table_config_add_storage(TableConfig* self, StorageConfig* config)
{
	list_append(&self->storages, &config->link);
	self->storages_count++;
}

static inline void
table_config_add_index(TableConfig* self, IndexConfig* config)
{
	list_append(&self->indexes, &config->link);
	self->indexes_count++;
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
	table_config_set_map(copy, mapping_copy(self->map));

	list_foreach(&self->indexes)
	{
		auto config = list_at(IndexConfig, link);
		auto config_copy = index_config_copy(config);
		table_config_add_index(copy, config_copy);
	}

	list_foreach(&self->storages)
	{
		auto config = list_at(StorageConfig, link);
		auto config_copy = storage_config_copy(config);
		table_config_add_storage(copy, config_copy);
	}
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

	// indexes
	data_skip(pos);
	data_read_array(pos, &count);
	while (count-- > 0)
	{
		auto config = index_config_read(pos);
		table_config_add_index(self, config);
	}

	// storages
	data_skip(pos);
	data_read_array(pos, &count);
	while (count-- > 0)
	{
		auto config = storage_config_read(pos);
		table_config_add_storage(self, config);
	}

	// map
	data_skip(pos);
	self->map = mapping_read(pos);

	return unguard(&self_guard);
}

static inline void
table_config_write(TableConfig* self, Buf* buf)
{
	// map
	encode_map(buf, 7);

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

	// indexes
	encode_raw(buf, "indexes", 7);
	encode_array(buf, self->indexes_count);
	list_foreach(&self->indexes)
	{
		auto config = list_at(IndexConfig, link);
		index_config_write(config, buf);
	}

	// storages
	encode_raw(buf, "storages", 8);
	encode_array(buf, self->storages_count);
	list_foreach(&self->storages)
	{
		auto config = list_at(StorageConfig, link);
		storage_config_write(config, buf);
	}

	// map
	encode_raw(buf, "map", 3);
	mapping_write(self->map, buf);
}
