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
