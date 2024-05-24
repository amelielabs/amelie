#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

static inline Buf*
table_op_create(TableConfig* config)
{
	// [config]
	auto buf = buf_begin();
	encode_array(buf);
	table_config_write(config, buf);
	encode_array_end(buf);
	return buf_end(buf);
}

static inline Buf*
table_op_drop(Str* schema, Str* name)
{
	// [schema, name]
	auto buf = buf_begin();
	encode_array(buf);
	encode_string(buf, schema);
	encode_string(buf, name);
	encode_array_end(buf);
	return buf_end(buf);
}

static inline Buf*
table_op_rename(Str* schema, Str* name, Str* schema_new, Str* name_new)
{
	// [schema, name, schema_new, name_new]
	auto buf = buf_begin();
	encode_array(buf);
	encode_string(buf, schema);
	encode_string(buf, name);
	encode_string(buf, schema_new);
	encode_string(buf, name_new);
	encode_array_end(buf);
	return buf_end(buf);
}

static inline TableConfig*
table_op_create_read(uint8_t** pos)
{
	data_read_array(pos);
	auto config = table_config_read(pos);
	guard(table_config_free, config);
	data_read_array_end(pos);
	unguard();
	return config;
}

static inline void
table_op_drop_read(uint8_t **pos, Str* schema, Str* name)
{
	data_read_array(pos);
	data_read_string(pos, schema);
	data_read_string(pos, name);
	data_read_array_end(pos);
}

static inline void
table_op_rename_read(uint8_t** pos, Str* schema, Str* name,
                     Str* schema_new, Str* name_new)
{
	data_read_array(pos);
	data_read_string(pos, schema);
	data_read_string(pos, name);
	data_read_string(pos, schema_new);
	data_read_string(pos, name_new);
	data_read_array_end(pos);
}
