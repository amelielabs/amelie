#pragma once

//
// monotone
//
// SQL OLTP database
//

static inline Buf*
view_op_create(ViewConfig* config)
{
	// [config]
	auto buf = buf_create(0);
	encode_array(buf, 1);
	view_config_write(config, buf);
	return buf;
}

static inline Buf*
view_op_drop(Str* schema, Str* name)
{
	// [schema, name]
	auto buf = buf_create(0);
	encode_array(buf, 2);
	encode_string(buf, schema);
	encode_string(buf, name);
	return buf;
}

static inline ViewConfig*
view_op_create_read(uint8_t** pos)
{
	int count;
	data_read_array(pos, &count);
	return view_config_read(pos);
}

static inline void
view_op_drop_read(uint8_t** pos, Str* schema, Str* name)
{
	int count;
	data_read_array(pos, &count);
	data_read_string(pos, schema);
	data_read_string(pos, name);
}
