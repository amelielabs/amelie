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
node_op_create(NodeConfig* config)
{
	// [config]
	auto buf = buf_create();
	encode_array(buf);
	node_config_write(config, buf);
	encode_array_end(buf);
	return buf;
}

static inline Buf*
node_op_drop(Str* name)
{
	// [name]
	auto buf = buf_create();
	encode_array(buf);
	encode_string(buf, name);
	encode_array_end(buf);
	return buf;
}

static inline NodeConfig*
node_op_create_read(uint8_t** pos)
{
	data_read_array(pos);
	auto config = node_config_read(pos);
	data_read_array_end(pos);
	return config;
}

static inline void
node_op_drop_read(uint8_t **pos, Str* name)
{
	data_read_array(pos);
	data_read_string(pos, name);
	data_read_array_end(pos);
}
