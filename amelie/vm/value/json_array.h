#pragma once

//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// Copyright (c) 2024 Amelie Labs.
//
// AGPL-3.0 Licensed.
//

static inline void
value_array(Value* result, Timezone* tz, Stack* stack, int count)
{
	auto buf = buf_create();
	encode_array(buf);
	for (int i = 0; i < count ; i++)
	{
		auto ref = stack_at(stack, count - i);
		value_encode(ref, tz, buf);
	}
	encode_array_end(buf);
	value_set_json_buf(result, buf);
}

static inline void
value_array_append(Value*   result, Timezone* tz,
                   uint8_t* data,
                   int      data_size,
                   int      argc,
                   Value*   argv)
{
	auto buf = buf_create();
	buf_write(buf, data, data_size - data_size_array_end());
	for (int i = 0; i < argc; i++)
		value_encode(&argv[i], tz, buf);
	encode_array_end(buf);
	value_set_json_buf(result, buf);
}

static inline void
value_array_push(Value*   result, Timezone* tz,
                 uint8_t* data,
                 int      data_size,
                 int      argc,
                 Value*   argv)
{
	auto buf = buf_create();
	encode_array(buf);
	for (int i = 0; i < argc; i++)
		value_encode(&argv[i], tz, buf);
	buf_write(buf, data + data_size_array(), data_size - data_size_array());
	value_set_json_buf(result, buf);
}

static inline void
value_array_pop(Value* result, uint8_t* data, int data_size)
{
	auto buf = buf_create();
	encode_array(buf);
	auto end = data + data_size;
	auto pos = data;
	data_read_array(&pos);
	if (! data_is_array_end(pos))
		data_skip(&pos);
	buf_write(buf, pos, end - pos);
	value_set_json_buf(result, buf);
}

static inline void
value_array_pop_back(Value* result, uint8_t* data)
{
	auto buf = buf_create();
	auto start = data;
	if (array_last(&data))
		buf_write(buf, start, data - start);
	else
		encode_array(buf);
	encode_array_end(buf);
	value_set_json_buf(result, buf);
}

static inline void
value_array_put(Value*   result, Timezone* tz,
                uint8_t* data,
                int      idx,
                Value*   value)
{
	data_read_array(&data);

	auto buf = buf_create();
	guard_buf(buf);

	encode_array(buf);
	int i = 0;
	while (! data_read_array_end(&data))
	{
		uint8_t* start = data;
		data_skip(&data);
		// replace
		if (i == idx)
			value_encode(value, tz, buf);
		else
			buf_write(buf, start, data - start);
		i++;
	}

	// extend by one
	if (idx < 0 || idx >= i)
		error("put: array index %d is out of bounds", idx);
	encode_array_end(buf);

	unguard();
	value_set_json_buf(result, buf);
}

static inline void
value_array_remove(Value* result, uint8_t* data, int idx)
{
	data_read_array(&data);

	auto buf = buf_create();
	guard_buf(buf);

	encode_array(buf);
	int i = 0;
	while (! data_read_array_end(&data))
	{
		uint8_t* start = data;
		data_skip(&data);
		if (i != idx)
			buf_write(buf, start, (data - start));
		i++;
	}
	if (idx < 0 || idx >= i)
		error("remove: array index %d is out of bounds", idx);
	encode_array_end(buf);

	unguard();
	value_set_json_buf(result, buf);
}
