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
data_skip(uint8_t** pos)
{
	int level = 0;
	do
	{
		switch (**pos) {
		case DATA_TRUE:
		case DATA_FALSE:
		{
			bool value;
			unpack_bool(pos, &value);
			break;
		}
		case DATA_NULL:
		{
			unpack_null(pos);
			break;
		}
		case DATA_REAL32:
		case DATA_REAL64:
		{
			double value;
			unpack_real(pos, &value);
			break;
		}
		case DATA_INTV0 ... DATA_INT64:
		{
			int64_t value;
			unpack_int(pos, &value);
			break;
		}
		case DATA_STRV0 ... DATA_STR32:
		{
			int   value_size;
			char* value;
			unpack_raw(pos, &value, &value_size);
			break;
		}
		case DATA_ARRAY:
		case DATA_OBJ:
			*pos += data_size_type();
			level++;
			break;
		case DATA_ARRAY_END:
		case DATA_OBJ_END:
			*pos += data_size_type();
			level--;
			break;
		default:
			data_error_read();
			break;
		}
	} while (level > 0);
}

hot static inline int
data_sizeof(uint8_t* data)
{
	auto pos = data;
	data_skip(&pos);
	return pos - data;
}

hot static inline bool
data_obj_find(uint8_t** pos, const char* name, int64_t name_size)
{
	if (unlikely(data_is_null(*pos)))
		return false;
	unpack_obj(pos);
	while (! unpack_obj_end(pos))
	{
		Str key;
		unpack_str(pos, &key);
		if (str_is(&key, name, name_size))
			return true;
		data_skip(pos);
	}
	return false;
}

hot static inline bool
data_obj_find_path(uint8_t** pos, Str* path)
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
			if (! data_obj_find(pos, current, left))
				return false;
			break;
		}
		if (! data_obj_find(pos, current, size))
			return false;
		current += (size + 1);
		left -= (size + 1);
	}
	return true;
}

static inline bool
data_obj_has(uint8_t* obj, Str* path)
{
	return data_obj_find_path(&obj, path) > 0;
}

hot static inline int
data_obj_size(uint8_t* pos)
{
	int count = 0;
	unpack_obj(&pos);
	while (! unpack_obj_end(&pos))
	{
		data_skip(&pos);
		data_skip(&pos);
		count++;
	}
	return count;
}

hot static inline bool
data_array_find(uint8_t** pos, int position)
{
	if (unlikely(data_is_null(*pos)))
		return false;
	unpack_array(pos);
	int i = 0;
	while (! unpack_array_end(pos))
	{
		if (i == position)
			return true;
		data_skip(pos);
		i++;
	}
	return false;
}

hot static inline bool
data_array_last(uint8_t** pos)
{
	if (unlikely(data_is_null(*pos)))
		return false;
	unpack_array(pos);
	while (! unpack_array_end(pos))
	{
		auto at = *pos;
		data_skip(&at);
		if (data_is_array_end(at))
			return true;
		*pos = at;
	}
	return false;
}

static inline bool
data_array_has(uint8_t* array, int position)
{
	return data_array_find(&array, position);
}

hot static inline int
data_array_size(uint8_t* pos)
{
	if (unlikely(data_is_null(pos)))
		return 0;
	int count = 0;
	unpack_array(&pos);
	while (! unpack_array_end(&pos))
	{
		data_skip(&pos);
		count++;
	}
	return count;
}
