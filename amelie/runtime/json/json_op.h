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

hot static inline void
json_skip(uint8_t** pos)
{
	int level = 0;
	do
	{
		switch (**pos) {
		case JSON_TRUE:
		case JSON_FALSE:
		{
			bool value;
			json_read_bool(pos, &value);
			break;
		}
		case JSON_NULL:
		{
			json_read_null(pos);
			break;
		}
		case JSON_REAL32:
		case JSON_REAL64:
		{
			double value;
			json_read_real(pos, &value);
			break;
		}
		case JSON_INTV0 ... JSON_INT64:
		{
			int64_t value;
			json_read_integer(pos, &value);
			break;
		}
		case JSON_STRINGV0 ... JSON_STRING32:
		{
			int   value_size;
			char* value;
			json_read_raw(pos, &value, &value_size);
			break;
		}
		case JSON_ARRAY:
		case JSON_OBJ:
			*pos += json_size_type();
			level++;
			break;
		case JSON_ARRAY_END:
		case JSON_OBJ_END:
			*pos += json_size_type();
			level--;
			break;
		default:
			json_error_read();
			break;
		}
	} while (level > 0);
}

hot static inline int
json_sizeof(uint8_t* data)
{
	auto pos = data;
	json_skip(&pos);
	return pos - data;
}

hot static inline bool
json_obj_find(uint8_t** pos, const char* name, int64_t name_size)
{
	if (unlikely(json_is_null(*pos)))
		return false;
	json_read_obj(pos);
	while (! json_read_obj_end(pos))
	{
		Str key;
		json_read_string(pos, &key);
		if (str_is(&key, name, name_size))
			return true;
		json_skip(pos);
	}
	return false;
}

hot static inline bool
json_obj_find_path(uint8_t** pos, Str* path)
{
	const char* current = str_of(path);
	int left = str_size(path);
	for (;;)
	{
		int size = -1;
		int i = 0;
		for (; i < left; i++) {
			if (current[i] == '.') {
				size = i;
				break;
			}
		}
		if (size == -1) {
			if (! json_obj_find(pos, current, left))
				return false;
			break;
		}
		if (! json_obj_find(pos, current, size))
			return false;
		current += (size + 1);
		left -= (size + 1);
	}
	return true;
}

static inline bool
json_obj_has(uint8_t* obj, Str* path)
{
	return json_obj_find_path(&obj, path) > 0;
}

hot static inline int
json_obj_size(uint8_t* pos)
{
	int count = 0;
	json_read_obj(&pos);
	while (! json_read_obj_end(&pos))
	{
		json_skip(&pos);
		json_skip(&pos);
		count++;
	}
	return count;
}

hot static inline bool
json_array_find(uint8_t** pos, int position)
{
	if (unlikely(json_is_null(*pos)))
		return false;
	json_read_array(pos);
	int i = 0;
	while (! json_read_array_end(pos))
	{
		if (i == position)
			return true;
		json_skip(pos);
		i++;
	}
	return false;
}

hot static inline bool
json_array_last(uint8_t** pos)
{
	if (unlikely(json_is_null(*pos)))
		return false;
	json_read_array(pos);
	while (! json_read_array_end(pos))
	{
		auto at = *pos;
		json_skip(&at);
		if (json_is_array_end(at))
			return true;
		*pos = at;
	}
	return false;
}

static inline bool
json_array_has(uint8_t* array, int position)
{
	return json_array_find(&array, position);
}

hot static inline int
json_array_size(uint8_t* pos)
{
	if (unlikely(json_is_null(pos)))
		return 0;
	int count = 0;
	json_read_array(&pos);
	while (! json_read_array_end(&pos))
	{
		json_skip(&pos);
		count++;
	}
	return count;
}
