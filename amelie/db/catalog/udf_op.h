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
udf_op_create(Buf* self, UdfConfig* config)
{
	// [op, config]
	auto offset = buf_size(self);
	encode_array(self);
	encode_integer(self, DDL_UDF_CREATE);
	udf_config_write(config, self);
	encode_array_end(self);
	return offset;
}

static inline UdfConfig*
udf_op_create_read(uint8_t* op)
{
	int64_t cmd;
	json_read_array(&op);
	json_read_integer(&op, &cmd);
	assert(cmd == DDL_UDF_CREATE);
	auto config = udf_config_read(&op);
	json_read_array_end(&op);
	return config;
}

static inline int
udf_op_drop(Buf* self, Str* schema, Str* name)
{
	// [op, schema, name]
	auto offset = buf_size(self);
	encode_array(self);
	encode_integer(self, DDL_UDF_DROP);
	encode_string(self, schema);
	encode_string(self, name);
	encode_array_end(self);
	return offset;
}

static inline void
udf_op_drop_read(uint8_t* op, Str* schema, Str* name)
{
	int64_t cmd;
	json_read_array(&op);
	json_read_integer(&op, &cmd);
	assert(cmd == DDL_UDF_DROP);
	json_read_string(&op, schema);
	json_read_string(&op, name);
	json_read_array_end(&op);
}

static inline int
udf_op_rename(Buf* self, Str* schema, Str* name, Str* schema_new, Str* name_new)
{
	// [op, schema, name, schema_new, name_new]
	auto offset = buf_size(self);
	encode_array(self);
	encode_integer(self, DDL_UDF_RENAME);
	encode_string(self, schema);
	encode_string(self, name);
	encode_string(self, schema_new);
	encode_string(self, name_new);
	encode_array_end(self);
	return offset;
}

static inline void
udf_op_rename_read(uint8_t* op, Str* schema, Str* name, Str* schema_new, Str* name_new)
{
	int64_t cmd;
	json_read_array(&op);
	json_read_integer(&op, &cmd);
	assert(cmd == DDL_UDF_RENAME);
	json_read_string(&op, schema);
	json_read_string(&op, name);
	json_read_string(&op, schema_new);
	json_read_string(&op, name_new);
	json_read_array_end(&op);
}
