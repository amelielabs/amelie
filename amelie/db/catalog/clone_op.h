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
clone_op_create(Buf* self, CloneConfig* config)
{
	// [op, config]
	auto offset = buf_size(self);
	encode_array(self);
	encode_int(self, DDL_CLONE_CREATE);
	clone_config_write(config, self, 0);
	encode_array_end(self);
	return offset;
}

static inline CloneConfig*
clone_op_create_read(uint8_t* op)
{
	int64_t cmd;
	unpack_array(&op);
	unpack_int(&op, &cmd);
	assert(cmd == DDL_CLONE_CREATE);
	auto config = clone_config_read(&op);
	unpack_array_end(&op);
	return config;
}
