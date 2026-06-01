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
rel_op_drop(Buf* self, RelType type, Str* user, Str* name, bool cascade)
{
	// [op, type, user, name, cascade]
	auto offset = buf_size(self);
	encode_array(self);
	encode_int(self, DDL_DROP);
	encode_int(self, type);
	encode_str(self, user);
	encode_str(self, name);
	encode_bool(self, cascade);
	encode_array_end(self);
	return offset;
}

static inline RelType
rel_op_drop_read(uint8_t* op, Str* user, Str* name, bool* cascade)
{
	int64_t cmd;
	int64_t type;
	unpack_array(&op);
	unpack_int(&op, &cmd);
	assert(cmd == DDL_DROP);
	unpack_int(&op, &type);
	unpack_str(&op, user);
	unpack_str(&op, name);
	unpack_bool(&op, cascade);
	unpack_array_end(&op);
	return type;
}

static inline int
rel_op_rename(Buf* self, RelType type, Str* user, Str* name, Str* user_new, Str* name_new)
{
	// [op, type, user, name, user_new, name_new]
	auto offset = buf_size(self);
	encode_array(self);
	encode_int(self, DDL_RENAME);
	encode_int(self, type);
	encode_str(self, user);
	encode_str(self, name);
	encode_str(self, user_new);
	encode_str(self, name_new);
	encode_array_end(self);
	return offset;
}

static inline RelType
rel_op_rename_read(uint8_t* op, Str* user, Str* name, Str* user_new, Str* name_new)
{
	int64_t cmd;
	int64_t type;
	unpack_array(&op);
	unpack_int(&op, &cmd);
	unpack_int(&op, &type);
	unpack_str(&op, user);
	unpack_str(&op, name);
	unpack_str(&op, user_new);
	unpack_str(&op, name_new);
	unpack_array_end(&op);
	return type;
}

static inline int
rel_op_grant(Buf* self, Str* user, Str* name, Str* to, bool grant, uint32_t perms)
{
	// [op, user, name, to, grant, perms]
	auto offset = buf_size(self);
	encode_array(self);
	encode_int(self, DDL_GRANT);
	encode_str(self, user);
	encode_str(self, name);
	encode_str(self, to);
	encode_bool(self, grant);
	encode_int(self, perms);
	encode_array_end(self);
	return offset;
}

static inline void
rel_op_grant_read(uint8_t* op, Str* user, Str* name, Str* to, bool* grant, int64_t* perms)
{
	int64_t cmd;
	unpack_array(&op);
	unpack_int(&op, &cmd);
	assert(cmd == DDL_GRANT);
	unpack_str(&op, user);
	unpack_str(&op, name);
	unpack_str(&op, to);
	unpack_bool(&op, grant);
	unpack_int(&op, perms);
	unpack_array_end(&op);
}

static inline int
rel_op_describe(Buf* self, RelType type, Str* user, Str* name, Str* description)
{
	// [op, type, user, name, description]
	auto offset = buf_size(self);
	encode_array(self);
	encode_int(self, DDL_DESCRIBE);
	encode_int(self, type);
	encode_str(self, user);
	encode_str(self, name);
	encode_str(self, description);
	encode_array_end(self);
	return offset;
}

static inline int
rel_op_describe_read(uint8_t* op, Str* user, Str* name, Str* description)
{
	int64_t cmd;
	int64_t type;
	unpack_array(&op);
	unpack_int(&op, &cmd);
	unpack_int(&op, &type);
	assert(cmd == DDL_DESCRIBE);
	unpack_str(&op, user);
	unpack_str(&op, name);
	unpack_str(&op, description);
	unpack_array_end(&op);
	return type;
}
