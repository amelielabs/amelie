#pragma once

//
// monotone
//
// SQL OLTP database
//

static inline Buf*
meta_op_create(MetaConfig* config)
{
	// [config]
	auto buf = buf_create(0);
	encode_array(buf, 1);
	meta_config_write(config, buf);
	return buf;
}

static inline Buf*
meta_op_drop(Str* name)
{
	// [name]
	auto buf = buf_create(0);
	encode_array(buf, 1);
	encode_string(buf, name);
	return buf;
}

static inline MetaConfig*
meta_op_create_read(uint8_t** pos)
{
	int count;
	data_read_array(pos, &count);
	return meta_config_read(pos);
}

static inline void
meta_op_drop_read(uint8_t** pos, Str* name)
{
	int count;
	data_read_array(pos, &count);
	data_read_string(pos, name);
}
