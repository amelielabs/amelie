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
schema_op_create(Buf* self, SchemaConfig* config)
{
	// [op, config]
	auto offset = buf_size(self);
	encode_array(self);
	encode_integer(self, DDL_SCHEMA_CREATE);
	schema_config_write(config, self);
	encode_array_end(self);
	return offset;
}

static inline SchemaConfig*
schema_op_create_read(uint8_t* op)
{
	int64_t cmd;
	json_read_array(&op);
	json_read_integer(&op, &cmd);
	assert(cmd == DDL_SCHEMA_CREATE);
	auto config = schema_config_read(&op);
	json_read_array_end(&op);
	return config;
}

static inline int
schema_op_drop(Buf* self, Str* name, bool cascade)
{
	// [op, name, cascade]
	auto offset = buf_size(self);
	encode_array(self);
	encode_integer(self, DDL_SCHEMA_DROP);
	encode_string(self, name);
	encode_bool(self, cascade);
	encode_array_end(self);
	return offset;
}

static inline void
schema_op_drop_read(uint8_t* op, Str* name, bool* cascade)
{
	int64_t cmd;
	json_read_array(&op);
	json_read_integer(&op, &cmd);
	assert(cmd == DDL_SCHEMA_DROP);
	json_read_string(&op, name);
	json_read_bool(&op, cascade);
	json_read_array_end(&op);
}

static inline int
schema_op_rename(Buf* self, Str* name, Str* name_new)
{
	// [op, name, name_new]
	auto offset = buf_size(self);
	encode_array(self);
	encode_integer(self, DDL_SCHEMA_RENAME);
	encode_string(self, name);
	encode_string(self, name_new);
	encode_array_end(self);
	return offset;
}

static inline void
schema_op_rename_read(uint8_t* op, Str* name, Str* name_new)
{
	int64_t cmd;
	json_read_array(&op);
	json_read_integer(&op, &cmd);
	assert(cmd == DDL_SCHEMA_RENAME);
	json_read_string(&op, name);
	json_read_string(&op, name_new);
	json_read_array_end(&op);
}
