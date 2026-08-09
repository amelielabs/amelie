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
	user_config_write(config, self, 0);
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
user_op_revoke_token(Buf* self, Str* name, Str* revoked_at)
{
	// [op, name, name_new]
	auto offset = buf_size(self);
	encode_array(self);
	encode_int(self, DDL_USER_REVOKE_TOKEN);
	encode_str(self, name);
	encode_str(self, revoked_at);
	encode_array_end(self);
	return offset;
}

static inline void
user_op_revoke_token_read(uint8_t* op, Str* name, Str* revoked_at)
{
	int64_t cmd;
	unpack_array(&op);
	unpack_int(&op, &cmd);
	assert(cmd == DDL_USER_REVOKE_TOKEN);
	unpack_str(&op, name);
	unpack_str(&op, revoked_at);
	unpack_array_end(&op);
}

static inline int
user_op_describe(Buf* self, Str* name, Str* description)
{
	// [op, name, description]
	auto offset = buf_size(self);
	encode_array(self);
	encode_int(self, DDL_USER_DESCRIBE);
	encode_str(self, name);
	encode_str(self, description);
	encode_array_end(self);
	return offset;
}

static inline void
user_op_describe_read(uint8_t* op, Str* name, Str* description)
{
	int64_t cmd;
	unpack_array(&op);
	unpack_int(&op, &cmd);
	assert(cmd == DDL_USER_DESCRIBE);
	unpack_str(&op, name);
	unpack_str(&op, description);
	unpack_array_end(&op);
}

static inline int
user_op_limit_set(Buf* self, Str* name, Limits* limits)
{
	// [op, name, limits]
	auto offset = buf_size(self);
	encode_array(self);
	encode_int(self, DDL_USER_LIMIT_SET);
	encode_str(self, name);
	limits_write(limits, self);
	encode_array_end(self);
	return offset;
}

static inline void
user_op_limit_set_read(uint8_t* op, Str* name, Limits* limits)
{
	int64_t cmd;
	unpack_array(&op);
	unpack_int(&op, &cmd);
	assert(cmd == DDL_USER_LIMIT_SET);
	unpack_str(&op, name);
	limits_read(limits, &op);
	unpack_array_end(&op);
}

static inline int
user_op_limit_unset(Buf* self, Str* name, uint64_t mask)
{
	// [op, name, mask]
	auto offset = buf_size(self);
	encode_array(self);
	encode_int(self, DDL_USER_LIMIT_UNSET);
	encode_str(self, name);
	encode_int(self, mask);
	encode_array_end(self);
	return offset;
}

static inline void
user_op_limit_unset_read(uint8_t* op, Str* name, uint64_t* mask)
{
	int64_t cmd;
	unpack_array(&op);
	unpack_int(&op, &cmd);
	assert(cmd == DDL_USER_LIMIT_UNSET);
	unpack_str(&op, name);
	int64_t value;
	unpack_int(&op, &value);
	*mask = (uint64_t)value;
	unpack_array_end(&op);
}
