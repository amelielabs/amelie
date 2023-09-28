#pragma once

//
// monotone
//
// SQL OLTP database
//

static inline void
value_map(Value* result, Stack* stack, int count)
{
	if (unlikely((count % 2) != 0))
		error("{}: incorrect map size");

	auto buf = msg_create(MSG_OBJECT);

	int actual_count = count / 2;
	encode_map(buf, actual_count);

	int i = 0;
	for (; i < count ; i++)
	{
		auto ref = stack_at(stack, count - i);
		if ((i % 2) == 0)
		{
			if (unlikely(ref->type != VALUE_STRING))
				error("{}: incorrect map key type");
		}
		value_write(ref, buf);
	}

	msg_end(buf);

	auto msg = msg_of(buf);
	value_set_data(result, msg->data, msg_data_size(msg), buf);
}

static inline void
value_map_has(Value* result, uint8_t* data, Str* path)
{
	value_set_bool(result, map_has(data, path));
}
