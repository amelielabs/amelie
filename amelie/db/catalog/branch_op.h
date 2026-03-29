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
branch_op_create(Buf* self, BranchConfig* config)
{
	// [op, config]
	auto offset = buf_size(self);
	encode_array(self);
	encode_integer(self, DDL_BRANCH_CREATE);
	branch_config_write(config, self, 0);
	encode_array_end(self);
	return offset;
}

static inline BranchConfig*
branch_op_create_read(uint8_t* op)
{
	int64_t cmd;
	json_read_array(&op);
	json_read_integer(&op, &cmd);
	assert(cmd == DDL_BRANCH_CREATE);
	auto config = branch_config_read(&op);
	json_read_array_end(&op);
	return config;
}

static inline int
branch_op_drop(Buf* self, Str* user, Str* name)
{
	// [op, user, name]
	auto offset = buf_size(self);
	encode_array(self);
	encode_integer(self, DDL_BRANCH_DROP);
	encode_string(self, user);
	encode_string(self, name);
	encode_array_end(self);
	return offset;
}

static inline void
branch_op_drop_read(uint8_t* op, Str* user, Str* name)
{
	int64_t cmd;
	json_read_array(&op);
	json_read_integer(&op, &cmd);
	assert(cmd == DDL_BRANCH_DROP);
	json_read_string(&op, user);
	json_read_string(&op, name);
	json_read_array_end(&op);
}

static inline int
branch_op_rename(Buf* self, Str* user, Str* name, Str* user_new, Str* name_new)
{
	// [op, user, name, user_new, name_new]
	auto offset = buf_size(self);
	encode_array(self);
	encode_integer(self, DDL_BRANCH_RENAME);
	encode_string(self, user);
	encode_string(self, name);
	encode_string(self, user_new);
	encode_string(self, name_new);
	encode_array_end(self);
	return offset;
}

static inline void
branch_op_rename_read(uint8_t* op, Str* user, Str* name, Str* user_new, Str* name_new)
{
	int64_t cmd;
	json_read_array(&op);
	json_read_integer(&op, &cmd);
	assert(cmd == DDL_BRANCH_RENAME);
	json_read_string(&op, user);
	json_read_string(&op, name);
	json_read_string(&op, user_new);
	json_read_string(&op, name_new);
	json_read_array_end(&op);
}
