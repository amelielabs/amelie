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
	encode_int(self, DDL_BRANCH_CREATE);
	branch_config_write(config, self, 0);
	encode_array_end(self);
	return offset;
}

static inline BranchConfig*
branch_op_create_read(uint8_t* op)
{
	int64_t cmd;
	unpack_array(&op);
	unpack_int(&op, &cmd);
	assert(cmd == DDL_BRANCH_CREATE);
	auto config = branch_config_read(&op);
	unpack_array_end(&op);
	return config;
}

static inline int
branch_op_drop(Buf* self, Str* user, Str* name)
{
	// [op, user, name]
	auto offset = buf_size(self);
	encode_array(self);
	encode_int(self, DDL_BRANCH_DROP);
	encode_str(self, user);
	encode_str(self, name);
	encode_array_end(self);
	return offset;
}

static inline void
branch_op_drop_read(uint8_t* op, Str* user, Str* name)
{
	int64_t cmd;
	unpack_array(&op);
	unpack_int(&op, &cmd);
	assert(cmd == DDL_BRANCH_DROP);
	unpack_str(&op, user);
	unpack_str(&op, name);
	unpack_array_end(&op);
}

static inline int
branch_op_rename(Buf* self, Str* user, Str* name, Str* user_new, Str* name_new)
{
	// [op, user, name, user_new, name_new]
	auto offset = buf_size(self);
	encode_array(self);
	encode_int(self, DDL_BRANCH_RENAME);
	encode_str(self, user);
	encode_str(self, name);
	encode_str(self, user_new);
	encode_str(self, name_new);
	encode_array_end(self);
	return offset;
}

static inline void
branch_op_rename_read(uint8_t* op, Str* user, Str* name, Str* user_new, Str* name_new)
{
	int64_t cmd;
	unpack_array(&op);
	unpack_int(&op, &cmd);
	assert(cmd == DDL_BRANCH_RENAME);
	unpack_str(&op, user);
	unpack_str(&op, name);
	unpack_str(&op, user_new);
	unpack_str(&op, name_new);
	unpack_array_end(&op);
}
