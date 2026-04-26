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
	encode_int(self, DDL_TABLE_CREATE);
	table_config_write(config, self, 0);
	encode_array_end(self);
	return offset;
}

static inline TableConfig*
table_op_create_read(uint8_t* op)
{
	int64_t cmd;
	unpack_array(&op);
	unpack_int(&op, &cmd);
	assert(cmd == DDL_TABLE_CREATE);
	auto config = table_config_read(&op);
	unpack_array_end(&op);
	return config;
}

static inline int
table_op_drop(Buf* self, Str* user, Str* name)
{
	// [op, user, name]
	auto offset = buf_size(self);
	encode_array(self);
	encode_int(self, DDL_TABLE_DROP);
	encode_str(self, user);
	encode_str(self, name);
	encode_array_end(self);
	return offset;
}

static inline void
table_op_drop_read(uint8_t* op, Str* user, Str* name)
{
	int64_t cmd;
	unpack_array(&op);
	unpack_int(&op, &cmd);
	assert(cmd == DDL_TABLE_DROP);
	unpack_str(&op, user);
	unpack_str(&op, name);
	unpack_array_end(&op);
}

static inline int
table_op_rename(Buf* self, Str* user, Str* name, Str* user_new, Str* name_new)
{
	// [op, user, name, user_new, name_new]
	auto offset = buf_size(self);
	encode_array(self);
	encode_int(self, DDL_TABLE_RENAME);
	encode_str(self, user);
	encode_str(self, name);
	encode_str(self, user_new);
	encode_str(self, name_new);
	encode_array_end(self);
	return offset;
}

static inline void
table_op_rename_read(uint8_t* op, Str* user, Str* name, Str* user_new, Str* name_new)
{
	int64_t cmd;
	unpack_array(&op);
	unpack_int(&op, &cmd);
	assert(cmd == DDL_TABLE_RENAME);
	unpack_str(&op, user);
	unpack_str(&op, name);
	unpack_str(&op, user_new);
	unpack_str(&op, name_new);
	unpack_array_end(&op);
}

static inline int
table_op_truncate(Buf* self, Str* user, Str* name)
{
	// [op, user, name]
	auto offset = buf_size(self);
	encode_array(self);
	encode_int(self, DDL_TABLE_TRUNCATE);
	encode_str(self, user);
	encode_str(self, name);
	encode_array_end(self);
	return offset;
}

static inline void
table_op_truncate_read(uint8_t* op, Str* user, Str* name)
{
	int64_t cmd;
	unpack_array(&op);
	unpack_int(&op, &cmd);
	assert(cmd == DDL_TABLE_TRUNCATE);
	unpack_str(&op, user);
	unpack_str(&op, name);
	unpack_array_end(&op);
}

static inline int
table_op_set_identity(Buf* self, Str* user, Str* name, int64_t value)
{
	// [op, user, name, value]
	auto offset = buf_size(self);
	encode_array(self);
	encode_int(self, DDL_TABLE_SET_IDENTITY);
	encode_str(self, user);
	encode_str(self, name);
	encode_int(self, value);
	encode_array_end(self);
	return offset;
}

static inline void
table_op_set_identity_read(uint8_t* op, Str* user, Str* name, int64_t* value)
{
	int64_t cmd;
	unpack_array(&op);
	unpack_int(&op, &cmd);
	assert(cmd == DDL_TABLE_SET_IDENTITY);
	unpack_str(&op, user);
	unpack_str(&op, name);
	unpack_int(&op, value);
	unpack_array_end(&op);
}

static inline int
table_op_column_add(Buf* self, Str* user, Str* name, Column* column)
{
	// [op, user, name, column]
	auto offset = buf_size(self);
	encode_array(self);
	encode_int(self, DDL_TABLE_COLUMN_ADD);
	encode_str(self, user);
	encode_str(self, name);
	column_write(column, self, 0);
	encode_array_end(self);
	return offset;
}

static inline Column*
table_op_column_add_read(uint8_t* op, Str* user, Str* name)
{
	int64_t cmd;
	unpack_array(&op);
	unpack_int(&op, &cmd);
	assert(cmd == DDL_TABLE_COLUMN_ADD);
	unpack_str(&op, user);
	unpack_str(&op, name);
	auto column = column_read(&op);
	unpack_array_end(&op);
	return column;
}

static inline int
table_op_column_drop(Buf* self, Str* user, Str* name, Str* name_column)
{
	// [op, user, name, column]
	auto offset = buf_size(self);
	encode_array(self);
	encode_int(self, DDL_TABLE_COLUMN_DROP);
	encode_str(self, user);
	encode_str(self, name);
	encode_str(self, name_column);
	encode_array_end(self);
	return offset;
}

static inline void
table_op_column_drop_read(uint8_t* op, Str* user, Str* name, Str* name_column)
{
	int64_t cmd;
	unpack_array(&op);
	unpack_int(&op, &cmd);
	assert(cmd == DDL_TABLE_COLUMN_DROP);
	unpack_str(&op, user);
	unpack_str(&op, name);
	unpack_str(&op, name_column);
	unpack_array_end(&op);
}

static inline int
table_op_column_set(Buf* self, int op, Str* user, Str* name, Str* column, Str* value)
{
	// [op, user, name, column, value]
	auto offset = buf_size(self);
	encode_array(self);
	encode_int(self, op);
	encode_str(self, user);
	encode_str(self, name);
	encode_str(self, column);
	encode_str(self, value);
	encode_array_end(self);
	return offset;
}

static inline int
table_op_column_set_read(uint8_t* op, Str* user, Str* name, Str* column, Str* value)
{
	int64_t cmd;
	unpack_array(&op);
	unpack_int(&op, &cmd);
	unpack_str(&op, user);
	unpack_str(&op, name);
	unpack_str(&op, column);
	unpack_str(&op, value);
	unpack_array_end(&op);
	return cmd;
}

static inline int
table_op_column_rename(Buf* self, Str* user, Str* name,
                       Str* name_column,
                       Str* name_column_new)
{
	// [op, user, name, column]
	auto offset = buf_size(self);
	encode_array(self);
	encode_int(self, DDL_TABLE_COLUMN_RENAME);
	encode_str(self, user);
	encode_str(self, name);
	encode_str(self, name_column);
	encode_str(self, name_column_new);
	encode_array_end(self);
	return offset;
}

static inline void
table_op_column_rename_read(uint8_t* op, Str* user, Str* name,
                            Str* name_column,
                            Str* name_column_new)
{
	int64_t cmd;
	unpack_array(&op);
	unpack_int(&op, &cmd);
	assert(cmd == DDL_TABLE_COLUMN_RENAME);
	unpack_str(&op, user);
	unpack_str(&op, name);
	unpack_str(&op, name_column);
	unpack_str(&op, name_column_new);
	unpack_array_end(&op);
}

static inline int
table_op_index_create(Buf* self, Str* user, Str* name, IndexConfig* config)
{
	// [op, user, name, config]
	auto offset = buf_size(self);
	encode_array(self);
	encode_int(self, DDL_INDEX_CREATE);
	encode_str(self, user);
	encode_str(self, name);
	index_config_write(config, self, 0);
	encode_array_end(self);
	return offset;
}

static inline uint8_t*
table_op_index_create_read(uint8_t* op, Str* user, Str* name)
{
	int64_t cmd;
	unpack_array(&op);
	unpack_int(&op, &cmd);
	assert(cmd == DDL_INDEX_CREATE);
	unpack_str(&op, user);
	unpack_str(&op, name);
	auto config_pos = op;
	data_skip(&op);
	unpack_array_end(&op);
	return config_pos;
}

static inline int
table_op_index_drop(Buf* self, Str* user, Str* name, Str* name_index)
{
	// [op, user, name, name_index]
	auto offset = buf_size(self);
	encode_array(self);
	encode_int(self, DDL_INDEX_DROP);
	encode_str(self, user);
	encode_str(self, name);
	encode_str(self, name_index);
	encode_array_end(self);
	return offset;
}

static inline void
table_op_index_drop_read(uint8_t* op, Str* user, Str* name, Str* name_index)
{
	int64_t cmd;
	unpack_array(&op);
	unpack_int(&op, &cmd);
	assert(cmd == DDL_INDEX_DROP);
	unpack_str(&op, user);
	unpack_str(&op, name);
	unpack_str(&op, name_index);
	unpack_array_end(&op);
}

static inline int
table_op_index_rename(Buf* self, Str* user, Str* name,
                      Str* name_index,
                      Str* name_index_new)
{
	// [op, user, name, name_index, name_index_new]
	auto offset = buf_size(self);
	encode_array(self);
	encode_int(self, DDL_INDEX_RENAME);
	encode_str(self, user);
	encode_str(self, name);
	encode_str(self, name_index);
	encode_str(self, name_index_new);
	encode_array_end(self);
	return offset;
}

static inline void
table_op_index_rename_read(uint8_t* op, Str* user, Str* name,
                           Str*     name_index,
                           Str*     name_index_new)
{
	int64_t cmd;
	unpack_array(&op);
	unpack_int(&op, &cmd);
	assert(cmd == DDL_INDEX_RENAME);
	unpack_str(&op, user);
	unpack_str(&op, name);
	unpack_str(&op, name_index);
	unpack_str(&op, name_index_new);
	unpack_array_end(&op);
}

static inline int
table_op_storage_add(Buf* self, Str* user, Str* table, Volume* config)
{
	// [op, user, table, storage]
	auto offset = buf_size(self);
	encode_array(self);
	encode_int(self, DDL_TABLE_STORAGE_ADD);
	encode_str(self, user);
	encode_str(self, table);
	volume_write(config, self, 0);
	encode_array_end(self);
	return offset;
}

static inline uint8_t*
table_op_storage_add_read(uint8_t* op, Str* user, Str* table)
{
	int64_t cmd;
	unpack_array(&op);
	unpack_int(&op, &cmd);
	assert(cmd == DDL_TABLE_STORAGE_ADD);
	unpack_str(&op, user);
	unpack_str(&op, table);
	auto config_pos = op;
	data_skip(&op);
	unpack_array_end(&op);
	return config_pos;
}

static inline int
table_op_storage_drop(Buf* self, Str* user, Str* table, Str* storage)
{
	// [op, user, table, storage]
	auto offset = buf_size(self);
	encode_array(self);
	encode_int(self, DDL_TABLE_STORAGE_DROP);
	encode_str(self, user);
	encode_str(self, table);
	encode_str(self, storage);
	encode_array_end(self);
	return offset;
}

static inline void
table_op_storage_drop_read(uint8_t* op, Str* user, Str* table, Str* storage)
{
	int64_t cmd;
	unpack_array(&op);
	unpack_int(&op, &cmd);
	assert(cmd == DDL_TABLE_STORAGE_DROP);
	unpack_str(&op, user);
	unpack_str(&op, table);
	unpack_str(&op, storage);
	unpack_array_end(&op);
}

static inline int
table_op_storage_pause(Buf* self, Str* user, Str* table, Str* storage, bool pause)
{
	// [op, user, table, storage, pause]
	auto offset = buf_size(self);
	encode_array(self);
	encode_int(self, DDL_TABLE_STORAGE_PAUSE);
	encode_str(self, user);
	encode_str(self, table);
	encode_str(self, storage);
	encode_bool(self, pause);
	encode_array_end(self);
	return offset;
}

static inline void
table_op_storage_pause_read(uint8_t* op, Str* user, Str* table, Str* storage,
                            bool* pause)
{
	int64_t cmd;
	unpack_array(&op);
	unpack_int(&op, &cmd);
	assert(cmd == DDL_TABLE_STORAGE_PAUSE);
	unpack_str(&op, user);
	unpack_str(&op, table);
	unpack_str(&op, storage);
	unpack_bool(&op, pause);
	unpack_array_end(&op);
}
