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
                   uint8_t* json,
                   int      json_size,
                   int      argc,
                   Value*   argv)
{
	auto buf = buf_create();
	buf_write(buf, json, json_size - json_size_array_end());
	for (int i = 0; i < argc; i++)
		value_encode(&argv[i], tz, buf);
	encode_array_end(buf);
	value_set_json_buf(result, buf);
}

static inline void
value_array_push(Value*   result, Timezone* tz,
                 uint8_t* json,
                 int      json_size,
                 int      argc,
                 Value*   argv)
{
	auto buf = buf_create();
	encode_array(buf);
	for (int i = 0; i < argc; i++)
		value_encode(&argv[i], tz, buf);
	buf_write(buf, json + json_size_array(), json_size - json_size_array());
	value_set_json_buf(result, buf);
}

static inline void
value_array_pop(Value* result, uint8_t* json, int json_size)
{
	auto buf = buf_create();
	encode_array(buf);
	auto end = json + json_size;
	auto pos = json;
	json_read_array(&pos);
	if (! json_is_array_end(pos))
		json_skip(&pos);
	buf_write(buf, pos, end - pos);
	value_set_json_buf(result, buf);
}

static inline void
value_array_pop_back(Value* result, uint8_t* json)
{
	auto buf = buf_create();
	auto start = json;
	if (json_array_last(&json))
		buf_write(buf, start, json - start);
	else
		encode_array(buf);
	encode_array_end(buf);
	value_set_json_buf(result, buf);
}

static inline void
value_array_put(Value*   result, Timezone* tz,
                uint8_t* json,
                int      idx,
                Value*   value)
{
	json_read_array(&json);

	auto buf = buf_create();
	errdefer_buf(buf);

	encode_array(buf);
	int i = 0;
	while (! json_read_array_end(&json))
	{
		uint8_t* start = json;
		json_skip(&json);
		// replace
		if (i == idx)
			value_encode(value, tz, buf);
		else
			buf_write(buf, start, json - start);
		i++;
	}

	// extend by one
	if (idx < 0 || idx >= i)
		error("put: array index %d is out of bounds", idx);
	encode_array_end(buf);

	value_set_json_buf(result, buf);
}

static inline void
value_array_remove(Value* result, uint8_t* json, int idx)
{
	json_read_array(&json);

	auto buf = buf_create();
	errdefer_buf(buf);

	encode_array(buf);
	int i = 0;
	while (! json_read_array_end(&json))
	{
		uint8_t* start = json;
		json_skip(&json);
		if (i != idx)
			buf_write(buf, start, (json - start));
		i++;
	}
	if (idx < 0 || idx >= i)
		error("remove: array index %d is out of bounds", idx);
	encode_array_end(buf);

	value_set_json_buf(result, buf);
}
