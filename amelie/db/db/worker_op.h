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

static inline Buf*
worker_op_create(WorkerConfig* config)
{
	// [config]
	auto buf = buf_create();
	encode_array(buf);
	worker_config_write(config, buf);
	encode_array_end(buf);
	return buf;
}

static inline Buf*
worker_op_drop(Str* name)
{
	// [name]
	auto buf = buf_create();
	encode_array(buf);
	encode_string(buf, name);
	encode_array_end(buf);
	return buf;
}

static inline WorkerConfig*
worker_op_create_read(uint8_t** pos)
{
	json_read_array(pos);
	auto config = worker_config_read(pos);
	json_read_array_end(pos);
	return config;
}

static inline void
worker_op_drop_read(uint8_t **pos, Str* name)
{
	json_read_array(pos);
	json_read_string(pos, name);
	json_read_array_end(pos);
}
