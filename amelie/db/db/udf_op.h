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
udf_op_create(UdfConfig* config)
{
	// [config]
	auto buf = buf_create();
	encode_array(buf);
	udf_config_write(config, buf);
	encode_array_end(buf);
	return buf;
}

static inline Buf*
udf_op_drop(Str* schema, Str* name)
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
udf_op_rename(Str* schema, Str* name, Str* schema_new, Str* name_new)
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

static inline UdfConfig*
udf_op_create_read(uint8_t** pos)
{
	json_read_array(pos);
	auto config = udf_config_read(pos);
	json_read_array_end(pos);
	return config;
}

static inline void
udf_op_drop_read(uint8_t** pos, Str* schema, Str* name)
{
	json_read_array(pos);
	json_read_string(pos, schema);
	json_read_string(pos, name);
	json_read_array_end(pos);
}

static inline void
udf_op_rename_read(uint8_t** pos, Str* schema, Str* name,
                   Str* schema_new, Str* name_new)
{
	json_read_array(pos);
	json_read_string(pos, schema);
	json_read_string(pos, name);
	json_read_string(pos, schema_new);
	json_read_string(pos, name_new);
	json_read_array_end(pos);
}
