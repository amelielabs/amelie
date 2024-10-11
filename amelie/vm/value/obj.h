#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

static inline void
value_obj(Value* result, Stack* stack, int count)
{
	if (unlikely((count % 2) != 0))
		error("{}: incorrect object size");

	auto buf = buf_create();
	guard_buf(buf);

	encode_obj(buf);
	for (int i = 0; i < count ; i++)
	{
		auto ref = stack_at(stack, count - i);
		if ((i % 2) == 0)
		{
			if (unlikely(ref->type != VALUE_STRING))
				error("{}: incorrect object key type");
		}
		value_write(ref, buf);
	}
	encode_obj_end(buf);

	unguard();
	value_set_obj_buf(result, buf);
}

static inline void
value_obj_has(Value* result, uint8_t* data, Str* path)
{
	value_set_bool(result, obj_has(data, path));
}
