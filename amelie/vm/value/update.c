
//
// amelie.
//
// Real-Time SQL Database.
//

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_data.h>
#include <amelie_config.h>
#include <amelie_row.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_value.h>

static inline bool
update_path_has(char** path, int* path_size)
{
	unused(path);
	return *path_size > 0;
}

static inline void
update_path_read(char** path, int* path_size, char** name, int* name_size)
{
	if (unlikely(! update_path_has(path, path_size)))
		error("update: incorrect path");

	char* pos = *path;
	int i = 0;
	for (; i < *path_size; i++)
	{
		if ((*path)[i] == '.')
		{
			*path      += 1;
			*path_size -= 1;
			break;
		}
		if (unlikely(isspace((*path)[i])))
			error("update: incorrect path");
	}
	*name       = pos;
	*name_size  = i;
	*path      += i;
	*path_size -= i;
}

hot static inline int
update_name_to_idx(const char* name, int name_size)
{
	int value = 0;
	const char* end = name + name_size;
	while (name < end) 
	{
		if (unlikely(! isdigit(*name)))
			error("update: incorrect array position");
		value = (value * 10) + *name - '0';
		name++;
	}
	return value;
}

hot static inline void
update_set_to(Buf*      buf,
              uint8_t** pos,
              char**    path,
              int*      path_size,
              Value*    value,
              bool*     found);

hot static inline void
update_set_to_array(Buf*      buf,
                    uint8_t** pos,
                    int       idx,
                    char**    path,
                    int*      path_size,
                    Value*    value,
                    bool*     found)
{
	if (unlikely(! data_is_array(*pos)))
		error("set: unexpected data type");
	data_read_array(pos);

	encode_array(buf);
	int i = 0;
	while (! data_read_array_end(pos))
	{
		// replace
		if (i == idx)
		{
			if (update_path_has(path, path_size))
			{
				update_set_to(buf, pos, path, path_size, value, found);
			} else
			{
				// replace value
				value_write(value, buf);
				assert(! *found);
				*found = true;
				data_skip(pos);
			}

			i++;
			continue;
		}

		uint8_t* start = *pos;
		data_skip(pos);
		buf_write(buf, start, *pos - start);
		i++;
	}

	// extend array by one
	if (idx == i)
	{
		if (update_path_has(path, path_size))
			error("set: incorrect path");
		value_write(value, buf);
		assert(! *found);
		*found = true;
	} else
	if (idx > i)
	{
		error("set: incorrect array position");
	}
	encode_array_end(buf);
}

hot static inline void
update_set_to_map(Buf*      buf,
                  uint8_t** pos,
                  char*     name,
                  int       name_size,
                  char**    path,
                  int*      path_size,
                  Value*    value,
                  bool*     found)
{
	if (unlikely(! data_is_map(*pos)))
		error("set: unexpected data type");
	data_read_map(pos);

	encode_map(buf);
	while (! data_read_map_end(pos))
	{
		// key 
		Str key;
		data_read_string(pos, &key);
		encode_string(buf, &key);

		// path match
		if (str_compare_raw(&key, name, name_size))
		{
			if (update_path_has(path, path_size))
			{
				update_set_to(buf, pos, path, path_size, value, found);
			} else
			{
				// replace value
				assert(! *found);
				value_write(value, buf);
				*found = true;
				data_skip(pos);
			}
			continue;
		}

		// value
		uint8_t *start = *pos;
		data_skip(pos);
		buf_write(buf, start, *pos - start);
	}

	if (! *found)
	{
		if (update_path_has(path, path_size))
			error("set: incorrect path");

		// key
		encode_raw(buf, name, name_size);

		// value
		value_write(value, buf);
	}
	encode_map_end(buf);
}

hot static inline void
update_set_to(Buf*      buf,
              uint8_t** pos,
              char**    path,
              int*      path_size,
              Value*    value,
              bool*     found)
{
	// iterate over path
	char* name;
	int   name_size;
	update_path_read(path, path_size, &name, &name_size);
	if (unlikely(name_size == 0))
		error("set: incorrect path");

	if (isdigit(*name))
	{
		// array
		int idx = update_name_to_idx(name, name_size);
		update_set_to_array(buf, pos, idx, path, path_size,
		                    value, found);
	} else
	{
		// map
		update_set_to_map(buf, pos, name, name_size,
		                  path, path_size,
		                  value, found);
	}
}

hot static inline void
update_unset_to(Buf* buf, uint8_t** pos, char** path, int* path_size);

hot static inline void
update_unset_to_array(Buf*      buf,
                      uint8_t** pos,
                      int       idx,
                      char**    path,
                      int*      path_size)
{
	if (unlikely(! data_is_array(*pos)))
		error("unset: unexpected data type");

	data_read_array(pos);

	encode_array(buf);
	if (update_path_has(path, path_size))
	{
		// replace array element
		int i = 0;
		while (! data_read_array_end(pos))
		{
			if (i == idx)
			{
				update_unset_to(buf, pos, path, path_size);
			} else
			{
				uint8_t* start = *pos;
				data_skip(pos);
				buf_write(buf, start, *pos - start);
			}
			i++;
		}

	} else
	{
		// remove array element
		int i = 0;
		while (! data_read_array_end(pos))
		{
			uint8_t* start = *pos;
			data_skip(pos);
			if (i != idx)
				buf_write(buf, start, *pos - start);
			i++;
		}
	}
	encode_array_end(buf);
}

hot static inline void
update_unset_to_map(Buf*      buf,
                    uint8_t** pos,
                    char*     name,
                    int       name_size,
                    char**    path,
                    int*      path_size)
{
	if (unlikely(! data_is_map(*pos)))
		error("unset: unexpected data type");
	data_read_map(pos);

	encode_map(buf);
	while (! data_read_map_end(pos))
	{
		// key
		Str key;
		data_read_string(pos, &key);

		// path match
		if (str_compare_raw(&key, name, name_size))
		{
			if (update_path_has(path, path_size))
			{
				encode_string(buf, &key);
				update_unset_to(buf, pos, path, path_size);
			} else
			{
				data_skip(pos);
			}
			continue;
		}

		// copy
		encode_string(buf, &key);
		uint8_t* start = *pos;
		data_skip(pos);
		buf_write(buf, start, *pos - start);
	}
	encode_map_end(buf);
}

hot static inline void
update_unset_to(Buf* buf, uint8_t** pos, char** path, int* path_size)
{
	// iterate over path
	char* name;
	int   name_size;
	update_path_read(path, path_size, &name, &name_size);
	if (unlikely(name_size == 0))
		error("unset: incorrect path");

	if (isdigit(*name))
	{
		// array
		int idx = update_name_to_idx(name, name_size);
		update_unset_to_array(buf, pos, idx, path, path_size);
	} else
	{
		// map
		update_unset_to_map(buf, pos, name, name_size,
		                    path,
		                    path_size);
	}
}

hot void
update_set(Value* result, uint8_t* data, Str* path, Value* value)
{
	auto  buf = buf_begin();
	bool  found = false;
	char* path_ptr = str_of(path);
	int   path_size = str_size(path);
	update_set_to(buf, &data, &path_ptr, &path_size, value, &found);
	buf_end(buf);
	value_set_buf(result, buf);
}

hot void
update_set_array(Value* result, uint8_t* data, int idx,
                 Str* path, Value* value)
{
	auto  buf = buf_begin();
	bool  found = false;
	char* path_ptr = str_of(path);
	int   path_size = str_size(path);
	update_set_to_array(buf, &data, idx, &path_ptr, &path_size, value, &found);
	buf_end(buf);
	value_set_buf(result, buf);
}

hot void
update_unset(Value* result, uint8_t* data, Str* path)
{
	auto  buf = buf_begin();
	char* path_ptr = str_of(path);
	int   path_size = str_size(path);
	update_unset_to(buf, &data, &path_ptr, &path_size);
	buf_end(buf);
	value_set_buf(result, buf);
}

hot void
update_unset_array(Value* result, uint8_t* data, int idx, Str* path)
{
	auto  buf = buf_begin();
	char* path_ptr = str_of(path);
	int   path_size = str_size(path);
	update_unset_to_array(buf, &data, idx, &path_ptr, &path_size);
	buf_end(buf);
	value_set_buf(result, buf);
}
