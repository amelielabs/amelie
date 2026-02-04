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

static inline int
table_op_create(Buf* self, TableConfig* config)
{
	// [op, config]
	auto offset = buf_size(self);
	encode_array(self);
	encode_integer(self, DDL_TABLE_CREATE);
	table_config_write(config, self, 0);
	encode_array_end(self);
	return offset;
}

static inline TableConfig*
table_op_create_read(uint8_t* op)
{
	int64_t cmd;
	json_read_array(&op);
	json_read_integer(&op, &cmd);
	assert(cmd == DDL_TABLE_CREATE);
	auto config = table_config_read(&op);
	json_read_array_end(&op);
	return config;
}

static inline int
table_op_drop(Buf* self, Str* db, Str* name)
{
	// [op, db, name]
	auto offset = buf_size(self);
	encode_array(self);
	encode_integer(self, DDL_TABLE_DROP);
	encode_string(self, db);
	encode_string(self, name);
	encode_array_end(self);
	return offset;
}

static inline void
table_op_drop_read(uint8_t* op, Str* db, Str* name)
{
	int64_t cmd;
	json_read_array(&op);
	json_read_integer(&op, &cmd);
	assert(cmd == DDL_TABLE_DROP);
	json_read_string(&op, db);
	json_read_string(&op, name);
	json_read_array_end(&op);
}

static inline int
table_op_rename(Buf* self, Str* db, Str* name, Str* db_new, Str* name_new)
{
	// [op, db, name, db_new, name_new]
	auto offset = buf_size(self);
	encode_array(self);
	encode_integer(self, DDL_TABLE_RENAME);
	encode_string(self, db);
	encode_string(self, name);
	encode_string(self, db_new);
	encode_string(self, name_new);
	encode_array_end(self);
	return offset;
}

static inline void
table_op_rename_read(uint8_t* op, Str* db, Str* name, Str* db_new, Str* name_new)
{
	int64_t cmd;
	json_read_array(&op);
	json_read_integer(&op, &cmd);
	assert(cmd == DDL_TABLE_RENAME);
	json_read_string(&op, db);
	json_read_string(&op, name);
	json_read_string(&op, db_new);
	json_read_string(&op, name_new);
	json_read_array_end(&op);
}

static inline int
table_op_truncate(Buf* self, Str* db, Str* name)
{
	// [op, db, name]
	auto offset = buf_size(self);
	encode_array(self);
	encode_integer(self, DDL_TABLE_TRUNCATE);
	encode_string(self, db);
	encode_string(self, name);
	encode_array_end(self);
	return offset;
}

static inline void
table_op_truncate_read(uint8_t* op, Str* db, Str* name)
{
	int64_t cmd;
	json_read_array(&op);
	json_read_integer(&op, &cmd);
	assert(cmd == DDL_TABLE_TRUNCATE);
	json_read_string(&op, db);
	json_read_string(&op, name);
	json_read_array_end(&op);
}

static inline int
table_op_set_identity(Buf* self, Str* db, Str* name, int64_t value)
{
	// [op, db, name, value]
	auto offset = buf_size(self);
	encode_array(self);
	encode_integer(self, DDL_TABLE_SET_IDENTITY);
	encode_string(self, db);
	encode_string(self, name);
	encode_integer(self, value);
	encode_array_end(self);
	return offset;
}

static inline void
table_op_set_identity_read(uint8_t* op, Str* db, Str* name, int64_t* value)
{
	int64_t cmd;
	json_read_array(&op);
	json_read_integer(&op, &cmd);
	assert(cmd == DDL_TABLE_SET_IDENTITY);
	json_read_string(&op, db);
	json_read_string(&op, name);
	json_read_integer(&op, value);
	json_read_array_end(&op);
}

static inline int
table_op_set_unlogged(Buf* self, Str* db, Str* name, bool value)
{
	// [op, db, name, value]
	auto offset = buf_size(self);
	encode_array(self);
	encode_integer(self, DDL_TABLE_SET_UNLOGGED);
	encode_string(self, db);
	encode_string(self, name);
	encode_bool(self, value);
	encode_array_end(self);
	return offset;
}

static inline void
table_op_set_unlogged_read(uint8_t* op, Str* db, Str* name, bool* value)
{
	int64_t cmd;
	json_read_array(&op);
	json_read_integer(&op, &cmd);
	assert(cmd == DDL_TABLE_SET_UNLOGGED);
	json_read_string(&op, db);
	json_read_string(&op, name);
	json_read_bool(&op, value);
	json_read_array_end(&op);
}

static inline int
table_op_column_add(Buf* self, Str* db, Str* name, Column* column)
{
	// [op, db, name, column]
	auto offset = buf_size(self);
	encode_array(self);
	encode_integer(self, DDL_TABLE_COLUMN_ADD);
	encode_string(self, db);
	encode_string(self, name);
	column_write(column, self, 0);
	encode_array_end(self);
	return offset;
}

static inline Column*
table_op_column_add_read(uint8_t* op, Str* db, Str* name)
{
	int64_t cmd;
	json_read_array(&op);
	json_read_integer(&op, &cmd);
	assert(cmd == DDL_TABLE_COLUMN_ADD);
	json_read_string(&op, db);
	json_read_string(&op, name);
	auto column = column_read(&op);
	json_read_array_end(&op);
	return column;
}

static inline int
table_op_column_drop(Buf* self, Str* db, Str* name, Str* name_column)
{
	// [op, db, name, column]
	auto offset = buf_size(self);
	encode_array(self);
	encode_integer(self, DDL_TABLE_COLUMN_DROP);
	encode_string(self, db);
	encode_string(self, name);
	encode_string(self, name_column);
	encode_array_end(self);
	return offset;
}

static inline void
table_op_column_drop_read(uint8_t* op, Str* db, Str* name, Str* name_column)
{
	int64_t cmd;
	json_read_array(&op);
	json_read_integer(&op, &cmd);
	assert(cmd == DDL_TABLE_COLUMN_DROP);
	json_read_string(&op, db);
	json_read_string(&op, name);
	json_read_string(&op, name_column);
	json_read_array_end(&op);
}

static inline int
table_op_column_set(Buf* self, int op, Str* db, Str* name, Str* column, Str* value)
{
	// [op, db, name, column, value]
	auto offset = buf_size(self);
	encode_array(self);
	encode_integer(self, op);
	encode_string(self, db);
	encode_string(self, name);
	encode_string(self, column);
	encode_string(self, value);
	encode_array_end(self);
	return offset;
}

static inline int
table_op_column_set_read(uint8_t* op, Str* db, Str* name, Str* column, Str* value)
{
	int64_t cmd;
	json_read_array(&op);
	json_read_integer(&op, &cmd);
	json_read_string(&op, db);
	json_read_string(&op, name);
	json_read_string(&op, column);
	json_read_string(&op, value);
	json_read_array_end(&op);
	return cmd;
}

static inline int
table_op_column_rename(Buf* self, Str* db, Str* name,
                       Str* name_column,
                       Str* name_column_new)
{
	// [op, db, name, column]
	auto offset = buf_size(self);
	encode_array(self);
	encode_integer(self, DDL_TABLE_COLUMN_RENAME);
	encode_string(self, db);
	encode_string(self, name);
	encode_string(self, name_column);
	encode_string(self, name_column_new);
	encode_array_end(self);
	return offset;
}

static inline void
table_op_column_rename_read(uint8_t* op, Str* db, Str* name,
                            Str* name_column,
                            Str* name_column_new)
{
	int64_t cmd;
	json_read_array(&op);
	json_read_integer(&op, &cmd);
	assert(cmd == DDL_TABLE_COLUMN_RENAME);
	json_read_string(&op, db);
	json_read_string(&op, name);
	json_read_string(&op, name_column);
	json_read_string(&op, name_column_new);
	json_read_array_end(&op);
}

static inline int
table_op_index_create(Buf* self, Str* db, Str* name, IndexConfig* config)
{
	// [op, db, name, config]
	auto offset = buf_size(self);
	encode_array(self);
	encode_integer(self, DDL_INDEX_CREATE);
	encode_string(self, db);
	encode_string(self, name);
	index_config_write(config, self, 0);
	encode_array_end(self);
	return offset;
}

static inline uint8_t*
table_op_index_create_read(uint8_t* op, Str* db, Str* name)
{
	int64_t cmd;
	json_read_array(&op);
	json_read_integer(&op, &cmd);
	assert(cmd == DDL_INDEX_CREATE);
	json_read_string(&op, db);
	json_read_string(&op, name);
	auto config_pos = op;
	json_skip(&op);
	json_read_array_end(&op);
	return config_pos;
}

static inline int
table_op_index_drop(Buf* self, Str* db, Str* name, Str* name_index)
{
	// [op, db, name, name_index]
	auto offset = buf_size(self);
	encode_array(self);
	encode_integer(self, DDL_INDEX_DROP);
	encode_string(self, db);
	encode_string(self, name);
	encode_string(self, name_index);
	encode_array_end(self);
	return offset;
}

static inline void
table_op_index_drop_read(uint8_t* op, Str* db, Str* name, Str* name_index)
{
	int64_t cmd;
	json_read_array(&op);
	json_read_integer(&op, &cmd);
	assert(cmd == DDL_INDEX_DROP);
	json_read_string(&op, db);
	json_read_string(&op, name);
	json_read_string(&op, name_index);
	json_read_array_end(&op);
}

static inline int
table_op_index_rename(Buf* self, Str* db, Str* name,
                      Str* name_index,
                      Str* name_index_new)
{
	// [op, db, name, name_index, name_index_new]
	auto offset = buf_size(self);
	encode_array(self);
	encode_integer(self, DDL_INDEX_RENAME);
	encode_string(self, db);
	encode_string(self, name);
	encode_string(self, name_index);
	encode_string(self, name_index_new);
	encode_array_end(self);
	return offset;
}

static inline void
table_op_index_rename_read(uint8_t* op, Str* db, Str* name,
                           Str*     name_index,
                           Str*     name_index_new)
{
	int64_t cmd;
	json_read_array(&op);
	json_read_integer(&op, &cmd);
	assert(cmd == DDL_INDEX_RENAME);
	json_read_string(&op, db);
	json_read_string(&op, name);
	json_read_string(&op, name_index);
	json_read_string(&op, name_index_new);
	json_read_array_end(&op);
}

static inline int
table_op_tier_create(Buf* self, Str* db, Str* name, Tier* tier)
{
	// [op, db, name, config]
	auto offset = buf_size(self);
	encode_array(self);
	encode_integer(self, DDL_TIER_CREATE);
	encode_string(self, db);
	encode_string(self, name);
	tier_write(tier, self, 0);
	encode_array_end(self);
	return offset;
}

static inline uint8_t*
table_op_tier_create_read(uint8_t* op, Str* db, Str* name)
{
	int64_t cmd;
	json_read_array(&op);
	json_read_integer(&op, &cmd);
	assert(cmd == DDL_TIER_CREATE);
	json_read_string(&op, db);
	json_read_string(&op, name);
	auto config_pos = op;
	json_skip(&op);
	json_read_array_end(&op);
	return config_pos;
}

static inline int
table_op_tier_drop(Buf* self, Str* db, Str* name, Str* name_tier)
{
	// [op, db, name, name_tier]
	auto offset = buf_size(self);
	encode_array(self);
	encode_integer(self, DDL_TIER_DROP);
	encode_string(self, db);
	encode_string(self, name);
	encode_string(self, name_tier);
	encode_array_end(self);
	return offset;
}

static inline void
table_op_tier_drop_read(uint8_t* op, Str* db, Str* name, Str* name_tier)
{
	int64_t cmd;
	json_read_array(&op);
	json_read_integer(&op, &cmd);
	assert(cmd == DDL_TIER_DROP);
	json_read_string(&op, db);
	json_read_string(&op, name);
	json_read_string(&op, name_tier);
	json_read_array_end(&op);
}

static inline int
table_op_tier_rename(Buf* self, Str* db, Str* name,
                     Str* name_tier,
                     Str* name_tier_new)
{
	// [op, db, name, name_tier, name_tier_new]
	auto offset = buf_size(self);
	encode_array(self);
	encode_integer(self, DDL_TIER_RENAME);
	encode_string(self, db);
	encode_string(self, name);
	encode_string(self, name_tier);
	encode_string(self, name_tier_new);
	encode_array_end(self);
	return offset;
}

static inline void
table_op_tier_rename_read(uint8_t* op, Str* db, Str* name,
                          Str*     name_tier,
                          Str*     name_tier_new)
{
	int64_t cmd;
	json_read_array(&op);
	json_read_integer(&op, &cmd);
	assert(cmd == DDL_TIER_RENAME);
	json_read_string(&op, db);
	json_read_string(&op, name);
	json_read_string(&op, name_tier);
	json_read_string(&op, name_tier_new);
	json_read_array_end(&op);
}

static inline int
table_op_tier_storage_add(Buf* self, Str* db, Str* table, Str* tier, Str* storage)
{
	// [op, db, table, storage]
	auto offset = buf_size(self);
	encode_array(self);
	encode_integer(self, DDL_TIER_STORAGE_ADD);
	encode_string(self, db);
	encode_string(self, table);
	encode_string(self, tier);
	encode_string(self, storage);
	encode_array_end(self);
	return offset;
}

static inline void
table_op_tier_storage_add_read(uint8_t* op, Str* db, Str* table, Str* tier, Str* storage)
{
	int64_t cmd;
	json_read_array(&op);
	json_read_integer(&op, &cmd);
	assert(cmd == DDL_TIER_STORAGE_ADD);
	json_read_string(&op, db);
	json_read_string(&op, table);
	json_read_string(&op, tier);
	json_read_string(&op, storage);
	json_read_array_end(&op);
}

static inline int
table_op_tier_storage_drop(Buf* self, Str* db, Str* table, Str* tier, Str* storage)
{
	// [op, db, table, storage]
	auto offset = buf_size(self);
	encode_array(self);
	encode_integer(self, DDL_TIER_STORAGE_DROP);
	encode_string(self, db);
	encode_string(self, table);
	encode_string(self, tier);
	encode_string(self, storage);
	encode_array_end(self);
	return offset;
}

static inline void
table_op_tier_storage_drop_read(uint8_t* op, Str* db, Str* table, Str* tier, Str* storage)
{
	int64_t cmd;
	json_read_array(&op);
	json_read_integer(&op, &cmd);
	assert(cmd == DDL_TIER_STORAGE_DROP);
	json_read_string(&op, db);
	json_read_string(&op, table);
	json_read_string(&op, tier);
	json_read_string(&op, storage);
	json_read_array_end(&op);
}

static inline int
table_op_tier_storage_pause(Buf* self, Str* db, Str* table, Str* tier, Str* storage, bool pause)
{
	// [op, db, table, storage, pause]
	auto offset = buf_size(self);
	encode_array(self);
	encode_integer(self, DDL_TIER_STORAGE_PAUSE);
	encode_string(self, db);
	encode_string(self, table);
	encode_string(self, tier);
	encode_string(self, storage);
	encode_bool(self, pause);
	encode_array_end(self);
	return offset;
}

static inline void
table_op_tier_storage_pause_read(uint8_t* op, Str* db, Str* table, Str* tier, Str* storage,
                                 bool* pause)
{
	int64_t cmd;
	json_read_array(&op);
	json_read_integer(&op, &cmd);
	assert(cmd == DDL_TIER_STORAGE_PAUSE);
	json_read_string(&op, db);
	json_read_string(&op, table);
	json_read_string(&op, tier);
	json_read_string(&op, storage);
	json_read_bool(&op, pause);
	json_read_array_end(&op);
}

static inline int
table_op_tier_set(Buf* self, Str* db, Str* name, Tier* tier, int mask)
{
	// [op, db, name, config, mask]
	auto offset = buf_size(self);
	encode_array(self);
	encode_integer(self, DDL_TIER_SET);
	encode_string(self, db);
	encode_string(self, name);
	tier_write(tier, self, 0);
	encode_integer(self, mask);
	encode_array_end(self);
	return offset;
}

static inline uint8_t*
table_op_tier_set_read(uint8_t* op, Str* db, Str* name, int64_t* mask)
{
	int64_t cmd;
	json_read_array(&op);
	json_read_integer(&op, &cmd);
	assert(cmd == DDL_TIER_SET);
	json_read_string(&op, db);
	json_read_string(&op, name);
	auto config_pos = op;
	json_skip(&op);
	json_read_integer(&op, mask);
	json_read_array_end(&op);
	return config_pos;
}
