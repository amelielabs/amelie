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
user_op_create(Buf* self, UserConfig* config)
{
	// [op, config]
	auto offset = buf_size(self);
	encode_array(self);
	encode_int(self, DDL_USER_CREATE);
	user_config_write(config, self, FSECRETS);
	encode_array_end(self);
	return offset;
}

static inline UserConfig*
user_op_create_read(uint8_t* op)
{
	int64_t cmd;
	unpack_array(&op);
	unpack_int(&op, &cmd);
	assert(cmd == DDL_USER_CREATE);
	auto config = user_config_read(&op);
	unpack_array_end(&op);
	return config;
}

static inline int
user_op_drop(Buf* self, Str* name, bool cascade)
{
	// [op, name, cascade]
	auto offset = buf_size(self);
	encode_array(self);
	encode_int(self, DDL_USER_DROP);
	encode_str(self, name);
	encode_bool(self, cascade);
	encode_array_end(self);
	return offset;
}

static inline void
user_op_drop_read(uint8_t* op, Str* name, bool* cascade)
{
	int64_t cmd;
	unpack_array(&op);
	unpack_int(&op, &cmd);
	assert(cmd == DDL_USER_DROP);
	unpack_str(&op, name);
	unpack_bool(&op, cascade);
	unpack_array_end(&op);
}

static inline int
user_op_rename(Buf* self, Str* name, Str* name_new)
{
	// [op, name, name_new]
	auto offset = buf_size(self);
	encode_array(self);
	encode_int(self, DDL_USER_RENAME);
	encode_str(self, name);
	encode_str(self, name_new);
	encode_array_end(self);
	return offset;
}

static inline void
user_op_rename_read(uint8_t* op, Str* name, Str* name_new)
{
	int64_t cmd;
	unpack_array(&op);
	unpack_int(&op, &cmd);
	assert(cmd == DDL_USER_RENAME);
	unpack_str(&op, name);
	unpack_str(&op, name_new);
	unpack_array_end(&op);
}

static inline int
user_op_revoke(Buf* self, Str* name, Str* revoked_at)
{
	// [op, name, name_new]
	auto offset = buf_size(self);
	encode_array(self);
	encode_int(self, DDL_USER_REVOKE);
	encode_str(self, name);
	encode_str(self, revoked_at);
	encode_array_end(self);
	return offset;
}

static inline void
user_op_revoke_read(uint8_t* op, Str* name, Str* revoked_at)
{
	int64_t cmd;
	unpack_array(&op);
	unpack_int(&op, &cmd);
	assert(cmd == DDL_USER_REVOKE);
	unpack_str(&op, name);
	unpack_str(&op, revoked_at);
	unpack_array_end(&op);
}
