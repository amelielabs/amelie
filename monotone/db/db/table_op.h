#pragma once

//
// indigo
//
// SQL OLTP database
//

static inline Buf*
table_op_create(TableConfig* config)
{
	// [config]
	auto buf = buf_create(0);
	encode_array(buf, 1);
	table_config_write(config, buf);
	return buf;
}

static inline Buf*
table_op_drop(Str* schema, Str* name)
{
	// [schema, name]
	auto buf = buf_create(0);
	encode_array(buf, 2);
	encode_string(buf, schema);
	encode_string(buf, name);
	return buf;
}

static inline Buf*
table_op_rename(Str* schema, Str* name, Str* schema_new, Str* name_new)
{
	// [schema, name, schema_new, name_new]
	auto buf = buf_create(0);
	encode_array(buf, 4);
	encode_string(buf, schema);
	encode_string(buf, name);
	encode_string(buf, schema_new);
	encode_string(buf, name_new);
	return buf;
}

static inline TableConfig*
table_op_create_read(uint8_t** pos)
{
	int count;
	data_read_array(pos, &count);
	return table_config_read(pos);
}

static inline void
table_op_drop_read(uint8_t **pos, Str* schema, Str* name)
{
	int count;
	data_read_array(pos, &count);
	data_read_string(pos, schema);
	data_read_string(pos, name);
}

static inline void
table_op_rename_read(uint8_t** pos, Str* schema, Str* name,
                     Str* schema_new, Str* name_new)
{
	int count;
	data_read_array(pos, &count);
	data_read_string(pos, schema);
	data_read_string(pos, name);
	data_read_string(pos, schema_new);
	data_read_string(pos, name_new);
}
