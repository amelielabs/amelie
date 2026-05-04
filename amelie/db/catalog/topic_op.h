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
