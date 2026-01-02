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
	Str     db;
	Str     name;
	Uuid    uuid;
	bool    unlogged;
	Columns columns;
	List    volumes;
	int     volumes_count;
	List    indexes;
	int     indexes_count;
};

static inline TableConfig*
table_config_allocate(void)
{
	TableConfig* self;
	self = am_malloc(sizeof(TableConfig));
	self->unlogged      = false;
	self->volumes_count = 0;
	self->indexes_count = 0;
	str_init(&self->db);
	str_init(&self->name);
	uuid_init(&self->uuid);
	columns_init(&self->columns);
	list_init(&self->volumes);
	list_init(&self->indexes);
	return self;
}

static inline void
table_config_free(TableConfig* self)
{
	str_free(&self->db);
	str_free(&self->name);

	list_foreach_safe(&self->indexes)
	{
		auto config = list_at(IndexConfig, link);
		index_config_free(config);
	}

	list_foreach_safe(&self->volumes)
	{
		auto config = list_at(VolumeConfig, link);
		volume_config_free(config);
	}

	columns_free(&self->columns);
	am_free(self);
}

static inline void
table_config_set_db(TableConfig* self, Str* db)
{
	str_free(&self->db);
	str_copy(&self->db, db);
}

static inline void
table_config_set_name(TableConfig* self, Str* name)
{
	str_free(&self->name);
	str_copy(&self->name, name);
}

static inline void
table_config_set_uuid(TableConfig* self, Uuid* uuid)
{
	self->uuid = *uuid;
}

static inline void
table_config_set_unlogged(TableConfig* self, bool value)
{
	self->unlogged = value;
}

static inline void
table_config_add_volume(TableConfig* self, VolumeConfig* config)
{
	list_append(&self->volumes, &config->link);
	self->volumes_count++;
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
	table_config_set_db(copy, &self->db);
	table_config_set_name(copy, &self->name);
	table_config_set_uuid(copy, &self->uuid);
	table_config_set_unlogged(copy, self->unlogged);
	columns_copy(&copy->columns, &self->columns);

	list_foreach(&self->volumes)
	{
		auto config = list_at(VolumeConfig, link);
		auto config_copy = volume_config_copy(config);
		table_config_add_volume(copy, config_copy);
	}

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
	return copy;
}

static inline TableConfig*
table_config_read(uint8_t** pos)
{
	auto self = table_config_allocate();
	errdefer(table_config_free, self);

	uint8_t* pos_columns = NULL;
	uint8_t* pos_indexes = NULL;
	uint8_t* pos_volumes = NULL;
	Decode obj[] =
	{
		{ DECODE_STRING, "db",       &self->db       },
		{ DECODE_STRING, "name",     &self->name     },
		{ DECODE_UUID,   "uuid",     &self->uuid     },
		{ DECODE_ARRAY,  "columns",  &pos_columns    },
		{ DECODE_BOOL,   "unlogged", &self->unlogged },
		{ DECODE_ARRAY,  "indexes",  &pos_indexes    },
		{ DECODE_ARRAY,  "volumes",  &pos_volumes    },
		{ 0,              NULL,       NULL           },
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

	// volumes
	json_read_array(&pos_volumes);
	while (! json_read_array_end(&pos_volumes))
	{
		auto config = volume_config_read(&pos_volumes);
		table_config_add_volume(self, config);
	}

	return self;
}

static inline void
table_config_write(TableConfig* self, Buf* buf)
{
	// obj
	encode_obj(buf);

	// db
	encode_raw(buf, "db", 2);
	encode_string(buf, &self->db);

	// name
	encode_raw(buf, "name", 4);
	encode_string(buf, &self->name);

	// uuid
	encode_raw(buf, "uuid", 4);
	encode_uuid(buf, &self->uuid);

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

	// volumes
	encode_raw(buf, "volumes", 7);
	encode_array(buf);
	list_foreach(&self->volumes)
	{
		auto config = list_at(VolumeConfig, link);
		volume_config_write(config, buf);
	}
	encode_array_end(buf);
	encode_obj_end(buf);
}

static inline void
table_config_write_compact(TableConfig* self, Buf* buf)
{
	// obj
	encode_obj(buf);

	// db
	encode_raw(buf, "db", 2);
	encode_string(buf, &self->db);

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
		if (str_compare_case(&config->name, name))
			return config;
	}
	return NULL;
}
