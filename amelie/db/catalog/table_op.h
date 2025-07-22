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
	table_config_write(config, self);
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
table_op_drop(Buf* self, Str* schema, Str* name)
{
	// [op, schema, name]
	auto offset = buf_size(self);
	encode_array(self);
	encode_integer(self, DDL_TABLE_DROP);
	encode_string(self, schema);
	encode_string(self, name);
	encode_array_end(self);
	return offset;
}

static inline void
table_op_drop_read(uint8_t* op, Str* schema, Str* name)
{
	int64_t cmd;
	json_read_array(&op);
	json_read_integer(&op, &cmd);
	assert(cmd == DDL_TABLE_DROP);
	json_read_string(&op, schema);
	json_read_string(&op, name);
	json_read_array_end(&op);
}

static inline int
table_op_rename(Buf* self, Str* schema, Str* name, Str* schema_new, Str* name_new)
{
	// [op, schema, name, schema_new, name_new]
	auto offset = buf_size(self);
	encode_array(self);
	encode_integer(self, DDL_TABLE_RENAME);
	encode_string(self, schema);
	encode_string(self, name);
	encode_string(self, schema_new);
	encode_string(self, name_new);
	encode_array_end(self);
	return offset;
}

static inline void
table_op_rename_read(uint8_t* op, Str* schema, Str* name, Str* schema_new, Str* name_new)
{
	int64_t cmd;
	json_read_array(&op);
	json_read_integer(&op, &cmd);
	assert(cmd == DDL_TABLE_RENAME);
	json_read_string(&op, schema);
	json_read_string(&op, name);
	json_read_string(&op, schema_new);
	json_read_string(&op, name_new);
	json_read_array_end(&op);
}

static inline int
table_op_truncate(Buf* self, Str* schema, Str* name)
{
	// [op, schema, name]
	auto offset = buf_size(self);
	encode_array(self);
	encode_integer(self, DDL_TABLE_TRUNCATE);
	encode_string(self, schema);
	encode_string(self, name);
	encode_array_end(self);
	return offset;
}

static inline void
table_op_truncate_read(uint8_t* op, Str* schema, Str* name)
{
	int64_t cmd;
	json_read_array(&op);
	json_read_integer(&op, &cmd);
	assert(cmd == DDL_TABLE_TRUNCATE);
	json_read_string(&op, schema);
	json_read_string(&op, name);
	json_read_array_end(&op);
}

static inline int
table_op_set_identity(Buf* self, Str* schema, Str* name, int64_t value)
{
	// [op, schema, name, value]
	auto offset = buf_size(self);
	encode_array(self);
	encode_integer(self, DDL_TABLE_SET_IDENTITY);
	encode_string(self, schema);
	encode_string(self, name);
	encode_integer(self, value);
	encode_array_end(self);
	return offset;
}

static inline void
table_op_set_identity_read(uint8_t* op, Str* schema, Str* name, int64_t* value)
{
	int64_t cmd;
	json_read_array(&op);
	json_read_integer(&op, &cmd);
	assert(cmd == DDL_TABLE_SET_IDENTITY);
	json_read_string(&op, schema);
	json_read_string(&op, name);
	json_read_integer(&op, value);
	json_read_array_end(&op);
}

static inline int
table_op_set_unlogged(Buf* self, Str* schema, Str* name, bool value)
{
	// [op, schema, name, value]
	auto offset = buf_size(self);
	encode_array(self);
	encode_integer(self, DDL_TABLE_SET_UNLOGGED);
	encode_string(self, schema);
	encode_string(self, name);
	encode_bool(self, value);
	encode_array_end(self);
	return offset;
}

static inline void
table_op_set_unlogged_read(uint8_t* op, Str* schema, Str* name, bool* value)
{
	int64_t cmd;
	json_read_array(&op);
	json_read_integer(&op, &cmd);
	assert(cmd == DDL_TABLE_SET_UNLOGGED);
	json_read_string(&op, schema);
	json_read_string(&op, name);
	json_read_bool(&op, value);
	json_read_array_end(&op);
}

static inline int
table_op_column_add(Buf* self, Str* schema, Str* name, Column* column)
{
	// [op, schema, name, column]
	auto offset = buf_size(self);
	encode_array(self);
	encode_integer(self, DDL_TABLE_COLUMN_ADD);
	encode_string(self, schema);
	encode_string(self, name);
	column_write(column, self);
	encode_array_end(self);
	return offset;
}

static inline Column*
table_op_column_add_read(uint8_t* op, Str* schema, Str* name)
{
	int64_t cmd;
	json_read_array(&op);
	json_read_integer(&op, &cmd);
	assert(cmd == DDL_TABLE_COLUMN_ADD);
	json_read_string(&op, schema);
	json_read_string(&op, name);
	auto column = column_read(&op);
	json_read_array_end(&op);
	return column;
}

static inline int
table_op_column_drop(Buf* self, Str* schema, Str* name, Str* name_column)
{
	// [op, schema, name, column]
	auto offset = buf_size(self);
	encode_array(self);
	encode_integer(self, DDL_TABLE_COLUMN_DROP);
	encode_string(self, schema);
	encode_string(self, name);
	encode_string(self, name_column);
	encode_array_end(self);
	return offset;
}

static inline void
table_op_column_drop_read(uint8_t* op, Str* schema, Str* name, Str* name_column)
{
	int64_t cmd;
	json_read_array(&op);
	json_read_integer(&op, &cmd);
	assert(cmd == DDL_TABLE_COLUMN_DROP);
	json_read_string(&op, schema);
	json_read_string(&op, name);
	json_read_string(&op, name_column);
	json_read_array_end(&op);
}

static inline int
table_op_column_set(Buf* self, int op, Str* schema, Str* name, Str* column, Str* value)
{
	// [op, schema, name, column, value]
	auto offset = buf_size(self);
	encode_array(self);
	encode_integer(self, op);
	encode_string(self, schema);
	encode_string(self, name);
	encode_string(self, column);
	encode_string(self, value);
	encode_array_end(self);
	return offset;
}

static inline int
table_op_column_set_read(uint8_t* op, Str* schema, Str* name, Str* column, Str* value)
{
	int64_t cmd;
	json_read_array(&op);
	json_read_integer(&op, &cmd);
	json_read_string(&op, schema);
	json_read_string(&op, name);
	json_read_string(&op, column);
	json_read_string(&op, value);
	json_read_array_end(&op);
	return cmd;
}

static inline int
table_op_column_rename(Buf* self, Str* schema, Str* name,
                       Str* name_column,
                       Str* name_column_new)
{
	// [op, schema, name, column]
	auto offset = buf_size(self);
	encode_array(self);
	encode_integer(self, DDL_TABLE_COLUMN_RENAME);
	encode_string(self, schema);
	encode_string(self, name);
	encode_string(self, name_column);
	encode_string(self, name_column_new);
	encode_array_end(self);
	return offset;
}

static inline void
table_op_column_rename_read(uint8_t* op, Str* schema, Str* name,
                            Str* name_column,
                            Str* name_column_new)
{
	int64_t cmd;
	json_read_array(&op);
	json_read_integer(&op, &cmd);
	assert(cmd == DDL_TABLE_COLUMN_RENAME);
	json_read_string(&op, schema);
	json_read_string(&op, name);
	json_read_string(&op, name_column);
	json_read_string(&op, name_column_new);
	json_read_array_end(&op);
}

static inline int
table_op_index_create(Buf* self, Str* schema, Str* name, IndexConfig* config)
{
	// [op, schema, name, config]
	auto offset = buf_size(self);
	encode_array(self);
	encode_integer(self, DDL_INDEX_CREATE);
	encode_string(self, schema);
	encode_string(self, name);
	index_config_write(config, self);
	encode_array_end(self);
	return offset;
}

static inline uint8_t*
table_op_index_create_read(uint8_t* op, Str* schema, Str* name)
{
	int64_t cmd;
	json_read_array(&op);
	json_read_integer(&op, &cmd);
	assert(cmd == DDL_INDEX_CREATE);
	json_read_string(&op, schema);
	json_read_string(&op, name);
	auto config_pos = op;
	json_read_array_end(&op);
	return config_pos;
}

static inline int
table_op_index_drop(Buf* self, Str* schema, Str* name, Str* name_index)
{
	// [op, schema, name, name_index]
	auto offset = buf_size(self);
	encode_array(self);
	encode_integer(self, DDL_INDEX_DROP);
	encode_string(self, schema);
	encode_string(self, name);
	encode_string(self, name_index);
	encode_array_end(self);
	return offset;
}

static inline void
table_op_index_drop_read(uint8_t* op, Str* schema, Str* name, Str* name_index)
{
	int64_t cmd;
	json_read_array(&op);
	json_read_integer(&op, &cmd);
	assert(cmd == DDL_INDEX_DROP);
	json_read_string(&op, schema);
	json_read_string(&op, name);
	json_read_string(&op, name_index);
	json_read_array_end(&op);
}

static inline int
table_op_index_rename(Buf* self, Str* schema, Str* name,
                      Str* name_index,
                      Str* name_index_new)
{
	// [op, schema, name, name_index, name_index_new]
	auto offset = buf_size(self);
	encode_array(self);
	encode_integer(self, DDL_INDEX_RENAME);
	encode_string(self, schema);
	encode_string(self, name);
	encode_string(self, name_index);
	encode_string(self, name_index_new);
	encode_array_end(self);
	return offset;
}

static inline void
table_op_index_rename_read(uint8_t* op, Str* schema, Str* name,
                           Str*     name_index,
                           Str*     name_index_new)
{
	int64_t cmd;
	json_read_array(&op);
	json_read_integer(&op, &cmd);
	assert(cmd == DDL_INDEX_RENAME);
	json_read_string(&op, schema);
	json_read_string(&op, name);
	json_read_string(&op, name_index);
	json_read_string(&op, name_index_new);
	json_read_array_end(&op);
}
