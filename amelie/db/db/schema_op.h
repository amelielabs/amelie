#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

static inline Buf*
schema_op_create(SchemaConfig* config)
{
	// [config]
	auto buf = buf_create();
	encode_array(buf);
	schema_config_write(config, buf);
	encode_array_end(buf);
	return buf;
}

static inline Buf*
schema_op_drop(Str* name)
{
	// [name]
	auto buf = buf_create();
	encode_array(buf);
	encode_string(buf, name);
	encode_array_end(buf);
	return buf;
}

static inline Buf*
schema_op_rename(Str* name, Str* name_new)
{
	// [name, name_new]
	auto buf = buf_create();
	encode_array(buf);
	encode_string(buf, name);
	encode_string(buf, name_new);
	encode_array_end(buf);
	return buf;
}

static inline SchemaConfig*
schema_op_create_read(uint8_t** pos)
{
	data_read_array(pos);
	auto config = schema_config_read(pos);
	data_read_array_end(pos);
	return config;
}

static inline void
schema_op_drop_read(uint8_t **pos, Str* name)
{
	data_read_array(pos);
	data_read_string(pos, name);
	data_read_array_end(pos);
}

static inline void
schema_op_rename_read(uint8_t** pos, Str* name, Str* name_new)
{
	data_read_array(pos);
	data_read_string(pos, name);
	data_read_string(pos, name_new);
	data_read_array_end(pos);
}
