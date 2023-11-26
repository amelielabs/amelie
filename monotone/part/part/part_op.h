#pragma once

//
// monotone
//
// SQL OLTP database
//

static inline Buf*
part_op_create(PartConfig* config)
{
	// [config]
	auto buf = buf_create(0);
	encode_array(buf, 1);
	part_config_write(config, buf);
	return buf;
}

static inline Buf*
part_op_drop(Uuid* id)
{
	// [id]
	auto buf = buf_create(0);
	encode_array(buf, 1);
	char uuid[UUID_SZ];
	uuid_to_string(id, uuid, sizeof(uuid));
	encode_raw(buf, uuid, sizeof(uuid) - 1);
	return buf;
}

static inline PartConfig*
part_op_create_read(uint8_t** pos)
{
	int count;
	data_read_array(pos, &count);
	return part_config_read(pos);
}

static inline void
part_op_drop_read(uint8_t** pos, Uuid* id)
{
	int count;
	data_read_array(pos, &count);
	Str uuid;
	data_read_string(pos, &uuid);
	uuid_from_string(id, &uuid);
}
