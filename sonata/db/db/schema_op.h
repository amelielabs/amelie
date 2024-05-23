#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

static inline Buf*
schema_op_create(SchemaConfig* config)
{
	// [config]
	auto buf = buf_begin();
	encode_array(buf, 1);
	schema_config_write(config, buf);
	return buf_end(buf);
}

static inline Buf*
schema_op_drop(Str* name)
{
	// [name]
	auto buf = buf_begin();
	encode_array(buf, 1);
	encode_string(buf, name);
	return buf_end(buf);
}

static inline Buf*
schema_op_rename(Str* name, Str* name_new)
{
	// [name, name_new]
	auto buf = buf_begin();
	encode_array(buf, 2);
	encode_string(buf, name);
	encode_string(buf, name_new);
	return buf_end(buf);
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

static inline void
schema_op_rename_read(uint8_t** pos, Str* name, Str* name_new)
{
	int count;
	data_read_array(pos, &count);
	data_read_string(pos, name);
	data_read_string(pos, name_new);
}
