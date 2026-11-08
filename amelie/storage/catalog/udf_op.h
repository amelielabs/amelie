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
udf_op_create(Buf* self, UdfConfig* config, bool or_create)
{
	// [op, config]
	auto offset = buf_size(self);
	encode_array(self);
	encode_int(self, DDL_UDF_CREATE);
	encode_bool(self, or_create);
	udf_config_write(config, self, 0);
	encode_array_end(self);
	return offset;
}

static inline UdfConfig*
udf_op_create_read(uint8_t* op, bool* or_replace)
{
	int64_t cmd;
	unpack_array(&op);
	unpack_int(&op, &cmd);
	assert(cmd == DDL_UDF_CREATE);
	unpack_bool(&op, or_replace);
	auto config = udf_config_read(&op);
	unpack_array_end(&op);
	return config;
}
