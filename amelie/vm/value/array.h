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
	value_set_array_buf(result, buf);
}

static inline void
value_array_append(Value*  result, uint8_t* data, int data_size,
                   int     argc,
                   Value** argv)
{
	auto buf = buf_begin();
	if (! data)
	{
		encode_array(buf);
		for (int i = 0; i < argc; i++)
			value_write(argv[i], buf);
		encode_array_end(buf);
	} else
	{
		buf_write(buf, data, data_size - data_size_array_end());
		for (int i = 0; i < argc; i++)
			value_write(argv[i], buf);
		encode_array_end(buf);
	}
	buf_end(buf);
	value_set_array_buf(result, buf);
}

static inline void
value_array_push(Value*  result, uint8_t* data, int data_size,
                 int     argc,
                 Value** argv)
{
	auto buf = buf_begin();
	encode_array(buf);
	for (int i = 0; i < argc; i++)
		value_write(argv[i], buf);
	buf_write(buf, data + data_size_array(), data_size - data_size_array());
	buf_end(buf);
	value_set_array_buf(result, buf);
}

static inline void
value_array_pop(Value* result, uint8_t* data, int data_size)
{
	auto buf = buf_begin();
	encode_array(buf);
	auto end = data + data_size;
	auto pos = data;
	data_read_array(&pos);
	if (! data_is_array_end(pos))
		data_skip(&pos);
	buf_write(buf, pos, end - pos);
	buf_end(buf);
	value_set_array_buf(result, buf);
}

static inline void
value_array_pop_back(Value* result, uint8_t* pos)
{
	auto buf = buf_begin();
	auto start = pos;
	if (array_last(&pos))
		buf_write(buf, start, pos - start);
	else
		encode_array(buf);
	encode_array_end(buf);
	buf_end(buf);
	value_set_array_buf(result, buf);
}

static inline void
value_array_put(Value* result, uint8_t* pos, int idx, Value* value)
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
	if (idx < 0 || idx >= i)
		error("put: array index '%d' is out of bounds", idx);
	encode_array_end(buf);

	buf_end(buf);
	value_set_array_buf(result, buf);
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
	value_set_array_buf(result, buf);
}
