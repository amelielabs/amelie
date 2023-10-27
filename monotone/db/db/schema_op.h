#pragma once

//
// monotone
//
// SQL OLTP database
//

static inline Buf*
schema_op_create(SchemaConfig* config)
{
	// [config]
	auto buf = buf_create(0);
	encode_array(buf, 1);
	schema_config_write(config, buf);
	return buf;
}

static inline Buf*
schema_op_drop(Str* name)
{
	// [name]
	auto buf = buf_create(0);
	encode_array(buf, 1);
	encode_string(buf, name);
	return buf;
}

static inline Buf*
schema_op_alter(Str* name, SchemaConfig* config)
{
	// [name, config]
	auto buf = buf_create(0);
	encode_array(buf, 2);
	encode_string(buf, name);
	schema_config_write(config, buf);
	return buf;
}

static inline SchemaConfig*
schema_op_create_read(uint8_t** pos)
{
	int count;
	data_read_array(pos, &count);
	return schema_config_read(pos);
}

static inline void
schema_op_drop_read(uint8_t **pos, Str* name)
{
	int count;
	data_read_array(pos, &count);
	data_read_string(pos, name);
}

static inline SchemaConfig*
schema_op_alter_read(uint8_t** pos, Str* name)
{
	int count;
	data_read_array(pos, &count);
	data_read_string(pos, name);
	return schema_config_read(pos);
}
