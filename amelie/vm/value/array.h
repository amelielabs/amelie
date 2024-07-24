#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

static inline void
value_array(Value* result, Stack* stack, int count)
{
	auto buf = buf_begin();
	encode_array(buf);
	for (int i = 0; i < count ; i++)
	{
		auto ref = stack_at(stack, count - i);
		value_write(ref, buf);
	}
	encode_array_end(buf);
	buf_end(buf);
	value_set_buf(result, buf);
}

static inline void
value_array_append(Value* result, uint8_t* data, int data_size, Value* value)
{
	auto buf = buf_begin();
	if (! data)
	{
		encode_array(buf);
		value_write(value, buf);
		encode_array_end(buf);
	} else
	{
		buf_write(buf, data, data_size - data_size_array_end());
		value_write(value, buf);
		encode_array_end(buf);
	}
	buf_end(buf);
	value_set_buf(result, buf);
}

static inline void
value_array_set(Value* result, uint8_t* pos, int idx, Value* value)
{
	data_read_array(&pos);

	auto buf = buf_begin();
	encode_array(buf);
	int i = 0;
	while (! data_read_array_end(&pos))
	{
		uint8_t* start = pos;
		data_skip(&pos);
		// replace
		if (i == idx)
			value_write(value, buf);
		else
			buf_write(buf, start, pos - start);
		i++;
	}

	// extend by one
	if (idx == i)
		value_write(value, buf);
	else
	if (idx < 0 || idx > i)
		error("<%d>: array index is out of bounds", idx);
	encode_array_end(buf);

	buf_end(buf);
	value_set_buf(result, buf);
}

static inline void
value_array_remove(Value* result, uint8_t* pos, int idx)
{
	data_read_array(&pos);

	auto buf = buf_begin();
	encode_array(buf);
	int i = 0;
	while (! data_read_array_end(&pos))
	{
		uint8_t* start = pos;
		data_skip(&pos);
		if (i != idx)
			buf_write(buf, start, (pos - start));
		i++;
	}
	if (idx < 0 || idx >= i)
		error("<%d>: array index is out of bounds", idx);
	encode_array_end(buf);

	buf_end(buf);
	value_set_buf(result, buf);
}

static inline void
value_array_has(Value* result, uint8_t* pos, int idx)
{
	bool found = false;
	int i = 0;
	data_read_array(&pos);
	while (! data_read_array_end(&pos))
	{
		if (i == idx) {
			found = true;
			break;
		}
		data_skip(&pos);
		i++;
	}
	value_set_bool(result, found);
}
