#pragma once

//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// AGPL-3.0 Licensed.
//

static inline Buf*
view_op_create(ViewConfig* config)
{
	// [config]
	auto buf = buf_create();
	encode_array(buf);
	view_config_write(config, buf);
	encode_array_end(buf);
	return buf;
}

static inline Buf*
view_op_drop(Str* schema, Str* name)
{
	// [schema, name]
	auto buf = buf_create();
	encode_array(buf);
	encode_string(buf, schema);
	encode_string(buf, name);
	encode_array_end(buf);
	return buf;
}

static inline Buf*
view_op_rename(Str* schema, Str* name, Str* schema_new, Str* name_new)
{
	// [schema, name, schema_new, name_new]
	auto buf = buf_create();
	encode_array(buf);
	encode_string(buf, schema);
	encode_string(buf, name);
	encode_string(buf, schema_new);
	encode_string(buf, name_new);
	encode_array_end(buf);
	return buf;
}

static inline ViewConfig*
view_op_create_read(uint8_t** pos)
{
	data_read_array(pos);
	auto config = view_config_read(pos);
	data_read_array_end(pos);
	return config;
}

static inline void
view_op_drop_read(uint8_t** pos, Str* schema, Str* name)
{
	data_read_array(pos);
	data_read_string(pos, schema);
	data_read_string(pos, name);
	data_read_array_end(pos);
}

static inline void
view_op_rename_read(uint8_t** pos, Str* schema, Str* name,
                    Str* schema_new, Str* name_new)
{
	data_read_array(pos);
	data_read_string(pos, schema);
	data_read_string(pos, name);
	data_read_string(pos, schema_new);
	data_read_string(pos, name_new);
	data_read_array_end(pos);
}
