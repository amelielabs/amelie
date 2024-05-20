#pragma once

//
// sonata.
//
// SQL Database for JSON.
//

static inline void
value_map(Value* result, Stack* stack, int count)
{
	if (unlikely((count % 2) != 0))
		error("{}: incorrect map size");

	auto buf = buf_begin();

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

	buf_end(buf);
	value_set_buf(result, buf);
}

static inline void
value_map_has(Value* result, uint8_t* data, Str* path)
{
	value_set_bool(result, map_has(data, path));
}
