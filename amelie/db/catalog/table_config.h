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
	Uuid    id;
	bool    unlogged;
	int64_t partitions;
	Columns columns;
	List    indexes;
	int     indexes_count;
	List    tiers;
	int     tiers_count;
};

static inline TableConfig*
table_config_allocate(void)
{
	TableConfig* self;
	self = am_malloc(sizeof(TableConfig));
	self->unlogged      = false;
	self->partitions    = 0;
	self->indexes_count = 0;
	self->tiers_count   = 0;
	str_init(&self->db);
	str_init(&self->name);
	uuid_init(&self->id);
	columns_init(&self->columns);
	list_init(&self->indexes);
	list_init(&self->tiers);
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

	list_foreach_safe(&self->tiers)
	{
		auto tier = list_at(Tier, link);
		tier_free(tier);
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
table_config_set_id(TableConfig* self, Uuid* id)
{
	self->id = *id;
}

static inline void
table_config_set_unlogged(TableConfig* self, bool value)
{
	self->unlogged = value;
}

static inline void
table_config_set_partitions(TableConfig* self, int value)
{
	self->partitions = value;
}

static inline void
table_config_index_add(TableConfig* self, IndexConfig* config)
{
	list_append(&self->indexes, &config->link);
	self->indexes_count++;
}

static inline void
table_config_index_remove(TableConfig* self, IndexConfig* config)
{
	list_unlink(&config->link);
	self->indexes_count--;
}

static inline void
table_config_tier_add(TableConfig* self, Tier* tier)
{
	list_append(&self->tiers, &tier->link);
	self->tiers_count++;
}

static inline void
table_config_tier_remove(TableConfig* self, Tier* tier)
{
	list_unlink(&tier->link);
	self->tiers_count--;
}

static inline TableConfig*
table_config_copy(TableConfig* self)
{
	auto copy = table_config_allocate();
	table_config_set_db(copy, &self->db);
	table_config_set_name(copy, &self->name);
	table_config_set_id(copy, &self->id);
	table_config_set_unlogged(copy, self->unlogged);
	table_config_set_partitions(copy, self->partitions);
	columns_copy(&copy->columns, &self->columns);

	Keys* primary_keys = NULL;
	list_foreach(&self->indexes)
	{
		auto config = list_at(IndexConfig, link);
		auto config_copy = index_config_copy(config, &copy->columns);
		table_config_index_add(copy, config_copy);
		keys_set_primary(&config_copy->keys, !primary_keys);
		if (primary_keys == NULL)
			primary_keys = &config_copy->keys;
	}

	list_foreach(&self->tiers)
	{
		auto tier = list_at(Tier, link);
		auto tier_dup = tier_copy(tier);
		table_config_tier_add(copy, tier_dup);
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
	uint8_t* pos_tiers   = NULL;
	Decode obj[] =
	{
		{ DECODE_STRING, "db",         &self->db         },
		{ DECODE_STRING, "name",       &self->name       },
		{ DECODE_UUID,   "id",         &self->id         },
		{ DECODE_ARRAY,  "columns",    &pos_columns      },
		{ DECODE_BOOL,   "unlogged",   &self->unlogged   },
		{ DECODE_INT,    "partitions", &self->partitions },
		{ DECODE_ARRAY,  "indexes",    &pos_indexes      },
		{ DECODE_ARRAY,  "tiers",      &pos_tiers        },
		{ 0,              NULL,         NULL             },
	};
	decode_obj(obj, "table", pos);

	// columns
	columns_read(&self->columns, &pos_columns);

	// indexes
	json_read_array(&pos_indexes);
	while (! json_read_array_end(&pos_indexes))
	{
		auto config = index_config_read(&self->columns, &pos_indexes);
		table_config_index_add(self, config);
	}

	// tiers
	json_read_array(&pos_tiers);
	while (! json_read_array_end(&pos_tiers))
	{
		auto tier = tier_read(&pos_tiers);
		table_config_tier_add(self, tier);
	}
	return self;
}

static inline void
table_config_write(TableConfig* self, Buf* buf, int flags)
{
	// obj
	encode_obj(buf);

	// db
	encode_raw(buf, "db", 2);
	encode_string(buf, &self->db);

	// name
	encode_raw(buf, "name", 4);
	encode_string(buf, &self->name);

	if (flags_has(flags, FMINIMAL))
	{
		encode_obj_end(buf);
		return;
	}

	// id
	encode_raw(buf, "id", 2);
	encode_uuid(buf, &self->id);

	// unlogged
	encode_raw(buf, "unlogged", 8);
	encode_bool(buf, self->unlogged);

	// partitions
	encode_raw(buf, "partitions", 10);
	encode_integer(buf, self->partitions);

	// columns
	encode_raw(buf, "columns", 7);
	columns_write(&self->columns, buf, flags);

	// indexes
	encode_raw(buf, "indexes", 7);
	encode_array(buf);
	list_foreach(&self->indexes)
	{
		auto config = list_at(IndexConfig, link);
		index_config_write(config, buf, flags);
	}
	encode_array_end(buf);

	// tiers
	encode_raw(buf, "tiers", 5);
	encode_array(buf);
	list_foreach(&self->tiers)
	{
		auto tier = list_at(Tier, link);
		tier_write(tier, buf, flags);
	}
	encode_array_end(buf);
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

static inline Tier*
table_config_find_tier(TableConfig* self, Str* name)
{
	list_foreach(&self->tiers)
	{
		auto tier = list_at(Tier, link);
		if (str_compare_case(&tier->name, name))
			return tier;
	}
	return NULL;
}
