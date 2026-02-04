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
storage_op_create(Buf* self, StorageConfig* config)
{
	// [op, config]
	auto offset = buf_size(self);
	encode_array(self);
	encode_integer(self, DDL_STORAGE_CREATE);
	storage_config_write(config, self, 0);
	encode_array_end(self);
	return offset;
}

static inline StorageConfig*
storage_op_create_read(uint8_t* op)
{
	int64_t cmd;
	json_read_array(&op);
	json_read_integer(&op, &cmd);
	assert(cmd == DDL_STORAGE_CREATE);
	auto config = storage_config_read(&op);
	json_read_array_end(&op);
	return config;
}

static inline int
storage_op_drop(Buf* self, Str* name)
{
	// [op, name]
	auto offset = buf_size(self);
	encode_array(self);
	encode_integer(self, DDL_STORAGE_DROP);
	encode_string(self, name);
	encode_array_end(self);
	return offset;
}

static inline void
storage_op_drop_read(uint8_t* op, Str* name)
{
	int64_t cmd;
	json_read_array(&op);
	json_read_integer(&op, &cmd);
	assert(cmd == DDL_STORAGE_DROP);
	json_read_string(&op, name);
	json_read_array_end(&op);
}

static inline int
storage_op_rename(Buf* self, Str* name, Str* name_new)
{
	// [op, name, name_new]
	auto offset = buf_size(self);
	encode_array(self);
	encode_integer(self, DDL_STORAGE_RENAME);
	encode_string(self, name);
	encode_string(self, name_new);
	encode_array_end(self);
	return offset;
}

static inline void
storage_op_rename_read(uint8_t* op, Str* name, Str* name_new)
{
	int64_t cmd;
	json_read_array(&op);
	json_read_integer(&op, &cmd);
	assert(cmd == DDL_STORAGE_RENAME);
	json_read_string(&op, name);
	json_read_string(&op, name_new);
	json_read_array_end(&op);
}
