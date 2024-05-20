#pragma once

//
// sonata.
//
// SQL Database for JSON.
//

always_inline hot static inline int
data_compare_integer_read(uint8_t** a, uint8_t** b)
{
	int64_t a_value;
	int64_t b_value;
	data_read_integer(a, &a_value);
	data_read_integer(b, &b_value);
	return compare_int64(a_value, b_value);
}

always_inline hot static inline int
data_compare_integer(uint8_t* a, uint8_t* b)
{
	return data_compare_integer_read(&a, &b);
}

always_inline hot static inline int
data_compare_string_read(uint8_t** a, uint8_t** b)
{
	char* a_value;
	int   a_value_size;
	char* b_value;
	int   b_value_size;
	data_read_raw(a, &a_value, &a_value_size);
	data_read_raw(b, &b_value, &b_value_size);
	int size;
	if (a_value_size < b_value_size)
		size = a_value_size;
	else
		size = b_value_size;
	int rc;
	rc = memcmp(a_value, b_value, size);
	if (rc == 0) {
		if (likely(a_value_size == b_value_size))
			return 0;
		return (a_value_size < b_value_size) ? -1 : 1;
	}
	return rc > 0 ? 1 : -1;
}

always_inline hot static inline int
data_compare_string(uint8_t* a, uint8_t* b)
{
	return data_compare_string_read(&a, &b);
}

hot static inline int
data_compare(uint8_t* a, uint8_t* b)
{
	switch (*a) {
	case SO_TRUE:
	case SO_FALSE:
	{
		if (! data_is_bool(b))
			return compare_int64(*a, *b);
		int a_value;
		int b_value;
		a_value = data_read_bool_at(a);
		b_value = data_read_bool_at(b);
		return compare_int64(a_value, b_value);
	}
	case SO_NULL:
	{
		if (! data_is_null(b))
			return compare_int64(*a, *b);
		return 0;
	}
	case SO_REAL32:
	case SO_REAL64:
	{
		if (! data_is_real(b))
			return compare_int64(*a, *b);
		double a_value;
		double b_value;
		a_value = data_read_real_at(a);
		b_value = data_read_real_at(b);
		if (a_value == b_value)
			return 0;
		return (a_value > b_value) ? 1 : -1;
	}
	case SO_INTV0 ... SO_INT64:
	{
		if (! data_is_integer(b))
			return compare_int64(*a, *b);
		return data_compare_integer(a, b);
	}
	case SO_STRINGV0 ... SO_STRING32:
	{
		if (! data_is_string(b))
			return compare_int64(*a, *b);
		return data_compare_string(a, b);
	}
	case SO_ARRAYV0 ... SO_ARRAY32:
	{
		if (! data_is_array(b))
			return compare_int64(*a, *b);
		int a_count;
		int b_count;
		data_read_array(&a, &a_count);
		data_read_array(&b, &b_count);
		if (a_count != b_count)
			return compare_int64(a_count, b_count);
		while (a_count-- > 0)
		{
			int rc = data_compare(a, b);
			if (rc != 0)
				return rc;
			data_skip(&a);
			data_skip(&b);
		}
		return 0;
	}
	case SO_MAPV0 ... SO_MAP32:
	{
		if (! data_is_map(b))
			return compare_int64(*a, *b);
		int a_count;
		int b_count;
		data_read_map(&a, &a_count);
		data_read_map(&b, &b_count);
		if (a_count != b_count)
			return compare_int64(a_count, b_count);
		while (a_count-- > 0)
		{
			// key
			int   key_size;
			char* key;
			data_read_raw(&a, &key, &key_size);

			// find key in b
			uint8_t* pos = b;
			if (! map_find(&pos, key, key_size))
				return 1;

			// compare
			int rc = data_compare(a, pos);
			if (rc != 0)
				return rc;

			data_skip(&a);
		}
		return 0;
	}
	default:
		break;
	}

	error_data();
	return -1;
}
