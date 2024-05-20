#pragma once

//
// indigo
//
// SQL OLTP database
//

hot static inline bool
map_find(uint8_t** pos, const char* name, int64_t name_size)
{
	int count;
	data_read_map(pos, &count);
	int i;
	for (i = 0; i < count; i++)
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
map_find_path(uint8_t** pos, Str* path)
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
			if (! map_find(pos, current, left))
				return false;
			break;
		}
		if (! map_find(pos, current, size))
			return false;
		current += (size + 1);
		left -= (size + 1);
	}
	return true;
}

static inline bool
map_has(uint8_t* map, Str* path)
{
	return map_find_path(&map, path) > 0;
}

hot static inline bool
array_find(uint8_t** pos, int position)
{
	int count;
	data_read_array(pos, &count);
	if (unlikely(count <= position))
		return false;
	int i;
	for (i = 0; i < position; i++)
		data_skip(pos);
	return true;
}

static inline bool
array_has(uint8_t* array, int idx)
{
	return array_find(&array, idx) > 0;
}
