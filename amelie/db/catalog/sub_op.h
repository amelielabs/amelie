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
sub_op_create(Buf* self, SubConfig* config)
{
	// [op, config]
	auto offset = buf_size(self);
	encode_array(self);
	encode_int(self, DDL_SUB_CREATE);
	sub_config_write(config, self, 0);
	encode_array_end(self);
	return offset;
}

static inline SubConfig*
sub_op_create_read(uint8_t* op)
{
	int64_t cmd;
	unpack_array(&op);
	unpack_int(&op, &cmd);
	assert(cmd == DDL_SUB_CREATE);
	auto config = sub_config_read(&op);
	unpack_array_end(&op);
	return config;
}

static inline int
sub_op_drop(Buf* self, Str* user, Str* name)
{
	// [op, user, name]
	auto offset = buf_size(self);
	encode_array(self);
	encode_int(self, DDL_SUB_DROP);
	encode_string(self, user);
	encode_string(self, name);
	encode_array_end(self);
	return offset;
}

static inline void
sub_op_drop_read(uint8_t* op, Str* user, Str* name)
{
	int64_t cmd;
	unpack_array(&op);
	unpack_int(&op, &cmd);
	assert(cmd == DDL_SUB_DROP);
	unpack_string(&op, user);
	unpack_string(&op, name);
	unpack_array_end(&op);
}

static inline int
sub_op_rename(Buf* self, Str* user, Str* name, Str* user_new, Str* name_new)
{
	// [op, user, name, user_new, name_new]
	auto offset = buf_size(self);
	encode_array(self);
	encode_int(self, DDL_SUB_RENAME);
	encode_string(self, user);
	encode_string(self, name);
	encode_string(self, user_new);
	encode_string(self, name_new);
	encode_array_end(self);
	return offset;
}

static inline void
sub_op_rename_read(uint8_t* op, Str* user, Str* name, Str* user_new, Str* name_new)
{
	int64_t cmd;
	unpack_array(&op);
	unpack_int(&op, &cmd);
	assert(cmd == DDL_SUB_RENAME);
	unpack_string(&op, user);
	unpack_string(&op, name);
	unpack_string(&op, user_new);
	unpack_string(&op, name_new);
	unpack_array_end(&op);
}

static inline int
acknowledge_op(Buf* self, uint64_t pos_lsn, uint32_t pos_op)
{
	auto offset = buf_size(self);
	encode_array(self);
	encode_int(self, pos_lsn);
	encode_int(self, pos_op);
	encode_array_end(self);
	return offset;
}

static inline void
acknowledge_op_read(uint8_t* op, int64_t* pos_lsn, int64_t* pos_op)
{
	unpack_array(&op);
	unpack_int(&op, pos_lsn);
	unpack_int(&op, pos_op);
	unpack_array_end(&op);
}
