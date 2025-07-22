#pragma once

//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// Copyright (c) 2024 Amelie Labs.
//
// AGPL-3.0 Licensed.
//

typedef struct TableConfig TableConfig;

struct TableConfig
{
	Str     schema;
	Str     name;
	bool    unlogged;
	Columns columns;
	List    indexes;
	int     indexes_count;
	List    partitions;
	int     partitions_count;
};

static inline TableConfig*
table_config_allocate(void)
{
	TableConfig* self;
	self = am_malloc(sizeof(TableConfig));
	self->unlogged         = false;
	self->indexes_count    = 0;
	self->partitions_count = 0;
	str_init(&self->schema);
	str_init(&self->name);
	columns_init(&self->columns);
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

	columns_free(&self->columns);
	am_free(self);
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
table_config_set_unlogged(TableConfig* self, bool value)
{
	self->unlogged = value;
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

static inline void
table_config_del_index(TableConfig* self, IndexConfig* config)
{
	list_unlink(&config->link);
	self->indexes_count--;
}

static inline TableConfig*
table_config_copy(TableConfig* self)
{
	auto copy = table_config_allocate();
	table_config_set_schema(copy, &self->schema);
	table_config_set_name(copy, &self->name);
	table_config_set_unlogged(copy, self->unlogged);
	columns_copy(&copy->columns, &self->columns);

	Keys* primary_keys = NULL;
	list_foreach(&self->indexes)
	{
		auto config = list_at(IndexConfig, link);
		auto config_copy = index_config_copy(config, &copy->columns);
		table_config_add_index(copy, config_copy);
		keys_set_primary(&config_copy->keys, !primary_keys);
		if (primary_keys == NULL)
			primary_keys = &config_copy->keys;
	}

	list_foreach(&self->partitions)
	{
		auto config = list_at(PartConfig, link);
		auto config_copy = part_config_copy(config);
		table_config_add_partition(copy, config_copy);
	}
	return copy;
}

static inline TableConfig*
table_config_read(uint8_t** pos)
{
	auto self = table_config_allocate();
	errdefer(table_config_free, self);

	uint8_t* pos_columns    = NULL;
	uint8_t* pos_indexes    = NULL;
	uint8_t* pos_partitions = NULL;
	Decode obj[] =
	{
		{ DECODE_STRING, "schema",     &self->schema     },
		{ DECODE_STRING, "name",       &self->name       },
		{ DECODE_ARRAY,  "columns",    &pos_columns      },
		{ DECODE_BOOL,   "unlogged",   &self->unlogged   },
		{ DECODE_ARRAY,  "indexes",    &pos_indexes      },
		{ DECODE_ARRAY,  "partitions", &pos_partitions   },
		{ 0,              NULL,        NULL              },
	};
	decode_obj(obj, "table", pos);

	// columns
	columns_read(&self->columns, &pos_columns);

	// indexes
	json_read_array(&pos_indexes);
	while (! json_read_array_end(&pos_indexes))
	{
		auto config = index_config_read(&self->columns, &pos_indexes);
		table_config_add_index(self, config);
	}

	// partitions
	json_read_array(&pos_partitions);
	while (! json_read_array_end(&pos_partitions))
	{
		auto config = part_config_read(&pos_partitions);
		table_config_add_partition(self, config);
	}

	return self;
}

static inline void
table_config_write(TableConfig* self, Buf* buf)
{
	// obj
	encode_obj(buf);

	// schema
	encode_raw(buf, "schema", 6);
	encode_string(buf, &self->schema);

	// name
	encode_raw(buf, "name", 4);
	encode_string(buf, &self->name);

	// unlogged
	encode_raw(buf, "unlogged", 8);
	encode_bool(buf, self->unlogged);

	// columns
	encode_raw(buf, "columns", 7);
	columns_write(&self->columns, buf);

	// indexes
	encode_raw(buf, "indexes", 7);
	encode_array(buf);
	list_foreach(&self->indexes)
	{
		auto config = list_at(IndexConfig, link);
		index_config_write(config, buf);
	}
	encode_array_end(buf);

	// partitions
	encode_raw(buf, "partitions", 10);
	encode_array(buf);
	list_foreach(&self->partitions)
	{
		auto config = list_at(PartConfig, link);
		part_config_write(config, buf);
	}
	encode_array_end(buf);
	encode_obj_end(buf);
}

static inline void
table_config_write_compact(TableConfig* self, Buf* buf)
{
	// obj
	encode_obj(buf);

	// schema
	encode_raw(buf, "schema", 6);
	encode_string(buf, &self->schema);

	// name
	encode_raw(buf, "name", 4);
	encode_string(buf, &self->name);

	// unlogged
	encode_raw(buf, "unlogged", 8);
	encode_bool(buf, self->unlogged);
	encode_obj_end(buf);
}

static inline IndexConfig*
table_config_find(TableConfig* self, Str* name)
{
	list_foreach(&self->indexes)
	{
		auto config = list_at(IndexConfig, link);
		if (str_compare(&config->name, name))
			return config;
	}
	return NULL;
}
