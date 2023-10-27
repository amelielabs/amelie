#pragma once

//
// monotone
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
table_op_alter(Str* schema, Str* name, TableConfig* config)
{
	// [schema, name, config]
	auto buf = buf_create(0);
	encode_array(buf, 3);
	encode_string(buf, schema);
	encode_string(buf, name);
	table_config_write(config, buf);
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

static inline TableConfig*
table_op_alter_read(uint8_t** pos, Str* schema, Str* name)
{
	int count;
	data_read_array(pos, &count);
	data_read_string(pos, schema);
	data_read_string(pos, name);
	return table_config_read(pos);
}
