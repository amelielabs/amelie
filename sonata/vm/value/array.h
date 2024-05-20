#pragma once

//
// sonata.
//
// SQL Database for JSON.
//

static inline void
value_array(Value* result, Stack* stack, int count)
{
	auto buf = buf_begin();
	encode_array(buf, count);
	int i = 0;
	for (; i < count ; i++)
	{
		auto ref = stack_at(stack, count - i);
		value_write(ref, buf);
	}
	buf_end(buf);
	value_set_buf(result, buf);
}

static inline void
value_array_set(Value* result, uint8_t* pos, int idx, Value* value)
{
	int count;
	data_read_array(&pos, &count);

	auto buf = buf_begin();
	if (idx >= 0 && idx < count)
	{
		// replace
		encode_array(buf, count);
		int i = 0;
		for (; i < count; i++)
		{
			uint8_t* start = pos;
			data_skip(&pos);
			if (i == idx)
				value_write(value, buf);
			else
				buf_write(buf, start, pos - start);
		}
	} else
	if (idx == count)
	{
		// extend by one
		encode_array(buf, count + 1);
		int i = 0;
		for (; i < count; i++)
		{
			uint8_t* start = pos;
			data_skip(&pos);
			buf_write(buf, start, pos - start);
		}
		value_write(value, buf);
	} else
	{
		error("<%d>: array index is out of bounds", idx);
	}
	buf_end(buf);
	value_set_buf(result, buf);
}

static inline void
value_array_remove(Value* result, uint8_t* pos, int idx)
{
	int count;
	data_read_array(&pos, &count);

	if (unlikely(count == 0))
		error("array is empty");

	if (idx < 0)
		idx = count + idx;
	if (idx < 0 || idx >= count)
		error("<%d>: array index is out of bounds", idx);

	auto buf = buf_begin();
	encode_array(buf, count - 1);
	int i = 0;
	for (; i < count; i++)
	{
		uint8_t* start = pos;
		data_skip(&pos);
		if (i == idx)
			continue;
		buf_write(buf, start, (pos - start));
	}
	buf_end(buf);
	value_set_buf(result, buf);
}

static inline void
value_array_has(Value* result, uint8_t* pos, int idx)
{
	int count;
	data_read_array(&pos, &count);
	if (idx < 0)
		idx = count + idx;
	bool not_exists = (idx < 0 || idx >= count);
	value_set_bool(result, !not_exists);
}
