#pragma once

//
// sonata.
//
// SQL Database for JSON.
//

typedef struct TableConfig TableConfig;

struct TableConfig
{
	Str  schema;
	Str  name;
	bool reference;
	List indexes;
	int  indexes_count;
	List partitions;
	int  partitions_count;
};

static inline TableConfig*
table_config_allocate(void)
{
	TableConfig* self;
	self = so_malloc(sizeof(TableConfig));
	self->reference        = false;
	self->indexes_count    = 0;
	self->partitions_count = 0;
	str_init(&self->schema);
	str_init(&self->name);
	list_init(&self->indexes);
	list_init(&self->partitions);
	return self;
}

static inline void
table_config_free(TableConfig* self)
{
	str_free(&self->schema);
	str_free(&self->name);

	list_foreach_safe(&self->indexes)
	{
		auto config = list_at(IndexConfig, link);
		index_config_free(config);
	}

	list_foreach_safe(&self->partitions)
	{
		auto config = list_at(PartConfig, link);
		part_config_free(config);
	}

	so_free(self);
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
table_config_add_partition(TableConfig* self, PartConfig* config)
{
	list_append(&self->partitions, &config->link);
	self->partitions_count++;
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
	guard(table_config_free, copy);
	table_config_set_schema(copy, &self->schema);
	table_config_set_name(copy, &self->name);
	table_config_set_reference(copy, self->reference);
	list_foreach(&self->indexes)
	{
		auto config = list_at(IndexConfig, link);
		auto config_copy = index_config_copy(config);
		table_config_add_index(copy, config_copy);
	}
	list_foreach(&self->partitions)
	{
		auto config = list_at(PartConfig, link);
		auto config_copy = part_config_copy(config);
		table_config_add_partition(copy, config_copy);
	}
	return unguard();
}

static inline TableConfig*
table_config_read(uint8_t** pos)
{
	auto self = table_config_allocate();
	guard(table_config_free, self);

	uint8_t* pos_indexes    = NULL;
	uint8_t* pos_partitions = NULL;
	Decode map[] =
	{
		{ DECODE_STRING, "schema",     &self->schema    },
		{ DECODE_STRING, "name",       &self->name      },
		{ DECODE_BOOL,   "reference",  &self->reference },
		{ DECODE_ARRAY,  "indexes",    &pos_indexes     },
		{ DECODE_ARRAY,  "partitions", &pos_partitions  },
		{ 0,              NULL,        NULL             },
	};
	decode_map(map, pos);

	// indexes
	int count;
	data_read_array(&pos_indexes, &count);
	while (count-- > 0)
	{
		auto config = index_config_read(&pos_indexes);
		table_config_add_index(self, config);
	}

	// partitions
	data_read_array(&pos_partitions, &count);
	while (count-- > 0)
	{
		auto config = part_config_read(&pos_partitions);
		table_config_add_partition(self, config);
	}

	return unguard();
}

static inline void
table_config_write(TableConfig* self, Buf* buf)
{
	// map
	encode_map(buf, 5);

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

	// partitions
	encode_raw(buf, "partitions", 10);
	encode_array(buf, self->partitions_count);
	list_foreach(&self->partitions)
	{
		auto config = list_at(PartConfig, link);
		part_config_write(config, buf);
	}
}
