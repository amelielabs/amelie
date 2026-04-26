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
topic_op_create(Buf* self, TopicConfig* config)
{
	// [op, config]
	auto offset = buf_size(self);
	encode_array(self);
	encode_int(self, DDL_TOPIC_CREATE);
	topic_config_write(config, self, 0);
	encode_array_end(self);
	return offset;
}

static inline TopicConfig*
topic_op_create_read(uint8_t* op)
{
	int64_t cmd;
	unpack_array(&op);
	unpack_int(&op, &cmd);
	assert(cmd == DDL_TOPIC_CREATE);
	auto config = topic_config_read(&op);
	unpack_array_end(&op);
	return config;
}

static inline int
topic_op_drop(Buf* self, Str* user, Str* name)
{
	// [op, user, name]
	auto offset = buf_size(self);
	encode_array(self);
	encode_int(self, DDL_TOPIC_DROP);
	encode_str(self, user);
	encode_str(self, name);
	encode_array_end(self);
	return offset;
}

static inline void
topic_op_drop_read(uint8_t* op, Str* user, Str* name)
{
	int64_t cmd;
	unpack_array(&op);
	unpack_int(&op, &cmd);
	assert(cmd == DDL_TOPIC_DROP);
	unpack_str(&op, user);
	unpack_str(&op, name);
	unpack_array_end(&op);
}

static inline int
topic_op_rename(Buf* self, Str* user, Str* name, Str* user_new, Str* name_new)
{
	// [op, user, name, user_new, name_new]
	auto offset = buf_size(self);
	encode_array(self);
	encode_int(self, DDL_TOPIC_RENAME);
	encode_str(self, user);
	encode_str(self, name);
	encode_str(self, user_new);
	encode_str(self, name_new);
	encode_array_end(self);
	return offset;
}

static inline void
topic_op_rename_read(uint8_t* op, Str* user, Str* name, Str* user_new, Str* name_new)
{
	int64_t cmd;
	unpack_array(&op);
	unpack_int(&op, &cmd);
	assert(cmd == DDL_TOPIC_RENAME);
	unpack_str(&op, user);
	unpack_str(&op, name);
	unpack_str(&op, user_new);
	unpack_str(&op, name_new);
	unpack_array_end(&op);
}
