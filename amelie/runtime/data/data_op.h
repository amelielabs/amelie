#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

hot static inline void
data_skip(uint8_t** pos)
{
	int level = 0;
	do
	{
		switch (**pos) {
		case AM_TRUE:
		case AM_FALSE:
		{
			bool value;
			data_read_bool(pos, &value);
			break;
		}
		case AM_NULL:
		{
			data_read_null(pos);
			break;
		}
		case AM_REAL32:
		case AM_REAL64:
		{
			double value;
			data_read_real(pos, &value);
			break;
		}
		case AM_INTV0 ... AM_INT64:
		{
			int64_t value;
			data_read_integer(pos, &value);
			break;
		}
		case AM_STRINGV0 ... AM_STRING32:
		{
			int   value_size;
			char* value;
			data_read_raw(pos, &value, &value_size);
			break;
		}
		case AM_ARRAY:
		case AM_OBJ:
			*pos += data_size_type();
			level++;
			break;
		case AM_ARRAY_END:
		case AM_OBJ_END:
			*pos += data_size_type();
			level--;
			break;
		case AM_INTERVAL:
		{
			*pos += data_size_type();
			int64_t value;
			data_read_integer(pos, &value);
			data_read_integer(pos, &value);
			data_read_integer(pos, &value);
			break;
		}
		case AM_TIMESTAMP:
		{
			int64_t value;
			data_read_timestamp(pos, &value);
			break;
		}
		case AM_VECTOR:
		{
			Vector vector;
			data_read_vector(pos, &vector);
			break;
		}
		default:
			error_data();
			break;
		}
	} while (level > 0);
}

hot static inline bool
obj_find(uint8_t** pos, const char* name, int64_t name_size)
{
	data_read_obj(pos);
	while (! data_read_obj_end(pos))
	{
		Str key;
		data_read_string(pos, &key);
		if (str_compare_raw(&key, name, name_size))
			return true;
		data_skip(pos);
	}
	return false;
}

hot static inline bool
obj_find_path(uint8_t** pos, Str* path)
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
			if (! obj_find(pos, current, left))
				return false;
			break;
		}
		if (! obj_find(pos, current, size))
			return false;
		current += (size + 1);
		left -= (size + 1);
	}
	return true;
}

static inline bool
obj_has(uint8_t* obj, Str* path)
{
	return obj_find_path(&obj, path) > 0;
}

hot static inline int
obj_size(uint8_t* pos)
{
	int count = 0;
	data_read_obj(&pos);
	while (! data_read_obj_end(&pos))
	{
		data_skip(&pos);
		data_skip(&pos);
		count++;
	}
	return count;
}

hot static inline bool
array_find(uint8_t** pos, int position)
{
	data_read_array(pos);
	int i = 0;
	while (! data_read_array_end(pos))
	{
		if (i == position)
			return true;
		data_skip(pos);
		i++;
	}
	return false;
}

hot static inline bool
array_last(uint8_t** pos)
{
	data_read_array(pos);
	while (! data_read_array_end(pos))
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
array_has(uint8_t* array, int position)
{
	return array_find(&array, position);
}

hot static inline int
array_size(uint8_t* pos)
{
	int count = 0;
	data_read_array(&pos);
	while (! data_read_array_end(&pos))
	{
		data_skip(&pos);
		count++;
	}
	return count;
}
