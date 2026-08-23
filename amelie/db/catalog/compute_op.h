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
compute_op_create(Buf* self, ComputeConfig* config)
{
	// [op, config]
	auto offset = buf_size(self);
	encode_array(self);
	encode_int(self, DDL_COMPUTE_CREATE);
	compute_config_write(config, self, 0);
	encode_array_end(self);
	return offset;
}

static inline ComputeConfig*
compute_op_create_read(uint8_t* op)
{
	int64_t cmd;
	unpack_array(&op);
	unpack_int(&op, &cmd);
	assert(cmd == DDL_COMPUTE_CREATE);
	auto config = compute_config_read(&op);
	unpack_array_end(&op);
	return config;
}
