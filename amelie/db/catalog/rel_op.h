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
rel_op_drop(Buf* self, RelType type, Str* name, bool cascade)
{
	// [op, type, name, cascade]
	auto offset = buf_size(self);
	encode_array(self);
	encode_int(self, DDL_DROP);
	encode_int(self, type);
	encode_str(self, name);
	encode_bool(self, cascade);
	encode_array_end(self);
	return offset;
}

static inline RelType
rel_op_drop_read(uint8_t* op, Str* name, bool* cascade)
{
	int64_t cmd;
	int64_t type;
	unpack_array(&op);
	unpack_int(&op, &cmd);
	assert(cmd == DDL_DROP);
	unpack_int(&op, &type);
	unpack_str(&op, name);
	unpack_bool(&op, cascade);
	unpack_array_end(&op);
	return type;
}

static inline int
rel_op_rename(Buf* self, RelType type, Str* name, Str* name_new)
{
	// [op, type, name, name_new]
	auto offset = buf_size(self);
	encode_array(self);
	encode_int(self, DDL_RENAME);
	encode_int(self, type);
	encode_str(self, name);
	encode_str(self, name_new);
	encode_array_end(self);
	return offset;
}

static inline RelType
rel_op_rename_read(uint8_t* op, Str* name, Str* name_new)
{
	int64_t cmd;
	int64_t type;
	unpack_array(&op);
	unpack_int(&op, &cmd);
	unpack_int(&op, &type);
	unpack_str(&op, name);
	unpack_str(&op, name_new);
	unpack_array_end(&op);
	return type;
}

static inline int
rel_op_describe(Buf* self, RelType type, Str* name, Str* description)
{
	// [op, type, name, description]
	auto offset = buf_size(self);
	encode_array(self);
	encode_int(self, DDL_DESCRIBE);
	encode_int(self, type);
	encode_str(self, name);
	encode_str(self, description);
	encode_array_end(self);
	return offset;
}

static inline int
rel_op_describe_read(uint8_t* op, Str* name, Str* description)
{
	int64_t cmd;
	int64_t type;
	unpack_array(&op);
	unpack_int(&op, &cmd);
	unpack_int(&op, &type);
	assert(cmd == DDL_DESCRIBE);
	unpack_str(&op, name);
	unpack_str(&op, description);
	unpack_array_end(&op);
	return type;
}
