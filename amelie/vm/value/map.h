#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

static inline void
value_map(Value* result, Stack* stack, int count)
{
	if (unlikely((count % 2) != 0))
		error("{}: incorrect map size");

	auto buf = buf_create();
	guard_buf(buf);

	encode_map(buf);
	for (int i = 0; i < count ; i++)
	{
		auto ref = stack_at(stack, count - i);
		if ((i % 2) == 0)
		{
			if (unlikely(ref->type != VALUE_STRING))
				error("{}: incorrect map key type");
		}
		value_write(ref, buf);
	}
	encode_map_end(buf);

	unguard();
	value_set_map_buf(result, buf);
}

static inline void
value_map_has(Value* result, uint8_t* data, Str* path)
{
	value_set_bool(result, map_has(data, path));
}
