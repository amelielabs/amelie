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
	Str          user;
	Str          name;
	Uuid         id;
	Columns      columns;
	List         indexes;
	int          indexes_count;
	bool         unlogged;
	Partitioning partitioning;
	Grants       grants;
};

static inline TableConfig*
table_config_allocate(void)
{
	TableConfig* self;
	self = am_malloc(sizeof(TableConfig));
	self->indexes_count = 0;
	self->unlogged      = false;
	str_init(&self->name);
	str_init(&self->user);
	uuid_init(&self->id);
	columns_init(&self->columns);
	partitioning_init(&self->partitioning);
	list_init(&self->indexes);
	grants_init(&self->grants);
	return self;
}

static inline void
table_config_free(TableConfig* self)
{
	str_free(&self->user);
	str_free(&self->name);

	list_foreach_safe(&self->indexes)
	{
		auto config = list_at(IndexConfig, link);
		index_config_free(config);
	}

	columns_free(&self->columns);
	partitioning_free(&self->partitioning);
	grants_free(&self->grants);
	am_free(self);
}

static inline void
table_config_set_user(TableConfig* self, Str* value)
{
	str_free(&self->user);
	str_copy(&self->user, value);
}

static inline void
table_config_set_name(TableConfig* self, Str* value)
{
	str_free(&self->name);
	str_copy(&self->name, value);
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

static inline TableConfig*
table_config_copy(TableConfig* self)
{
	auto copy = table_config_allocate();
	table_config_set_name(copy, &self->name);
	table_config_set_user(copy, &self->user);
	table_config_set_id(copy, &self->id);
	table_config_set_unlogged(copy, self->unlogged);
	partitioning_copy(&self->partitioning, &copy->partitioning);
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
	grants_copy(&copy->grants, &self->grants);
	return copy;
}

static inline TableConfig*
table_config_read(uint8_t** pos)
{
	auto self = table_config_allocate();
	errdefer(table_config_free, self);

	uint8_t* pos_columns      = NULL;
	uint8_t* pos_indexes      = NULL;
	uint8_t* pos_partitioning = NULL;
	uint8_t* pos_grants       = NULL;
	Decode obj[] =
	{
		{ DECODE_STR,   "user",         &self->user       },
		{ DECODE_STR,   "name",         &self->name       },
		{ DECODE_UUID,  "id",           &self->id         },
		{ DECODE_BOOL,  "unlogged",     &self->unlogged   },
		{ DECODE_ARRAY, "columns",      &pos_columns      },
		{ DECODE_ARRAY, "indexes",      &pos_indexes      },
		{ DECODE_OBJ,   "partitioning", &pos_partitioning },
		{ DECODE_ARRAY, "grants",       &pos_grants       },
		{ 0,             NULL,           NULL             },
	};
	decode_obj(obj, "table", pos);

	// columns
	columns_read(&self->columns, &pos_columns);

	// indexes
	unpack_array(&pos_indexes);
	while (! unpack_array_end(&pos_indexes))
	{
		auto config = index_config_read(&self->columns, &pos_indexes);
		table_config_index_add(self, config);
	}

	// partitioning
	partitioning_read(&self->partitioning, &pos_partitioning);

	// grants
	grants_read(&self->grants, &pos_grants);
	return self;
}

static inline void
table_config_write(TableConfig* self, Buf* buf, int flags)
{
	// obj
	encode_obj(buf);

	// user
	encode_raw(buf, "user", 4);
	encode_str(buf, &self->user);

	// name
	encode_raw(buf, "name", 4);
	encode_str(buf, &self->name);

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

	// partitioning
	encode_raw(buf, "partitioning", 12);
	partitioning_write(&self->partitioning, buf, flags);

	// grants
	encode_raw(buf, "grants", 6);
	grants_write(&self->grants, buf, 0);

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
