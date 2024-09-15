#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

static inline Buf*
table_op_create(TableConfig* config)
{
	// [config]
	auto buf = buf_create();
	encode_array(buf);
	table_config_write(config, buf);
	encode_array_end(buf);
	return buf;
}

static inline Buf*
table_op_drop(Str* schema, Str* name)
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
table_op_rename(Str* schema, Str* name, Str* schema_new, Str* name_new)
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

static inline Buf*
table_op_truncate(Str* schema, Str* name)
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
table_op_column_rename(Str* schema, Str* name,
                       Str* name_column,
                       Str* name_column_new)
{
	// [schema, name, column]
	auto buf = buf_create();
	encode_array(buf);
	encode_string(buf, schema);
	encode_string(buf, name);
	encode_string(buf, name_column);
	encode_string(buf, name_column_new);
	encode_array_end(buf);
	return buf;
}

static inline Buf*
table_op_column_add(Str* schema, Str* name, Column* column)
{
	// [schema, name, column]
	auto buf = buf_create();
	encode_array(buf);
	encode_string(buf, schema);
	encode_string(buf, name);
	column_write(column, buf);
	encode_array_end(buf);
	return buf;
}

static inline Buf*
table_op_column_drop(Str* schema, Str* name, Str* name_column)
{
	// [schema, name, column]
	auto buf = buf_create();
	encode_array(buf);
	encode_string(buf, schema);
	encode_string(buf, name);
	encode_string(buf, name_column);
	encode_array_end(buf);
	return buf;
}

static inline TableConfig*
table_op_create_read(uint8_t** pos)
{
	data_read_array(pos);
	auto config = table_config_read(pos);
	data_read_array_end(pos);
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

static inline void
table_op_truncate_read(uint8_t** pos, Str* schema, Str* name)
{
	data_read_array(pos);
	data_read_string(pos, schema);
	data_read_string(pos, name);
	data_read_array_end(pos);
}

static inline void
table_op_column_rename_read(uint8_t** pos, Str* schema, Str* name,
                            Str* name_column,
                            Str* name_column_new)
{
	data_read_array(pos);
	data_read_string(pos, schema);
	data_read_string(pos, name);
	data_read_string(pos, name_column);
	data_read_string(pos, name_column_new);
	data_read_array_end(pos);
}

static inline Column*
table_op_column_add_read(uint8_t** pos, Str* schema, Str* name)
{
	data_read_array(pos);
	data_read_string(pos, schema);
	data_read_string(pos, name);
	auto config = column_read(pos);
	data_read_array_end(pos);
	return config;
}

static inline void
table_op_column_drop_read(uint8_t** pos, Str* schema, Str* name, Str* name_column)
{
	data_read_array(pos);
	data_read_string(pos, schema);
	data_read_string(pos, name);
	data_read_string(pos, name_column);
	data_read_array_end(pos);
}

static inline Buf*
table_op_create_index(Str* schema, Str* name, IndexConfig* config)
{
	// [schema, name, config]
	auto buf = buf_create();
	encode_array(buf);
	encode_string(buf, schema);
	encode_string(buf, name);
	index_config_write(config, buf);
	encode_array_end(buf);
	return buf;
}

static inline Buf*
table_op_drop_index(Str* schema, Str* name, Str* index)
{
	// [schema, name, index_name]
	auto buf = buf_create();
	encode_array(buf);
	encode_string(buf, schema);
	encode_string(buf, name);
	encode_string(buf, index);
	encode_array_end(buf);
	return buf;
}

static inline Buf*
table_op_rename_index(Str* schema, Str* name, Str* index, Str* index_new)
{
	// [schema, name, index_name, index_name_new]
	auto buf = buf_create();
	encode_array(buf);
	encode_string(buf, schema);
	encode_string(buf, name);
	encode_string(buf, index);
	encode_string(buf, index_new);
	encode_array_end(buf);
	return buf;
}

static inline uint8_t*
table_op_create_index_read(uint8_t** pos, Str* schema, Str* name)
{
	data_read_array(pos);
	data_read_string(pos, schema);
	data_read_string(pos, name);
	auto config_pos = *pos;
	data_skip(pos);
	data_read_array_end(pos);
	return config_pos;
}

static inline void
table_op_drop_index_read(uint8_t **pos, Str* schema, Str* name, Str* index)
{
	data_read_array(pos);
	data_read_string(pos, schema);
	data_read_string(pos, name);
	data_read_string(pos, index);
	data_read_array_end(pos);
}

static inline void
table_op_rename_index_read(uint8_t **pos, Str* schema, Str* name,
                           Str* index,
                           Str* index_new)
{
	data_read_array(pos);
	data_read_string(pos, schema);
	data_read_string(pos, name);
	data_read_string(pos, index);
	data_read_string(pos, index_new);
	data_read_array_end(pos);
}
