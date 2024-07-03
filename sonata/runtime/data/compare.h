#pragma once

//
// sonata.
//
// Real-Time SQL Database.
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
	int level = 0;
	int rc;
	do
	{
		switch (*a) {
		case SO_TRUE:
		case SO_FALSE:
		{
			if (! data_is_bool(b))
				return compare_int64(*a, *b);
			bool a_value;
			bool b_value;
			data_read_bool(&a, &a_value);
			data_read_bool(&b, &b_value);
			rc = compare_int64(a_value, b_value);
			if (rc != 0)
				return rc;
			break;
		}
		case SO_NULL:
		{
			if (! data_is_null(b))
				return compare_int64(*a, *b);
			data_skip(&a);
			data_skip(&b);
			break;
		}
		case SO_REAL32:
		case SO_REAL64:
		{
			if (! data_is_real(b))
				return compare_int64(*a, *b);
			double a_value;
			double b_value;
			data_read_real(&a, &a_value);
			data_read_real(&b, &b_value);
			if (a_value == b_value)
				break;
			return (a_value > b_value) ? 1 : -1;
		}
		case SO_INTV0 ... SO_INT64:
		{
			if (! data_is_integer(b))
				return compare_int64(*a, *b);
			rc = data_compare_integer(a, b);
			if (rc != 0)
				return rc;
			break;
		}
		case SO_STRINGV0 ... SO_STRING32:
		{
			if (! data_is_string(b))
				return compare_int64(*a, *b);
			rc = data_compare_string(a, b);
			if (rc != 0)
				return rc;
			break;
		}
		case SO_ARRAY:
		{
			if (! data_is_array(b))
				return compare_int64(*a, *b);
			data_read_array(&a);
			data_read_array(&b);
			level++;
			break;
		}
		case SO_ARRAY_END:
		{
			if (! data_is_array_end(b))
				return compare_int64(*a, *b);
			data_read_array_end(&a);
			data_read_array_end(&b);
			level--;
			break;
		}
		case SO_MAP:
		{
			if (! data_is_map(b))
				return compare_int64(*a, *b);
			data_read_map(&a);
			data_read_map(&b);
			level++;
			break;
		}
		case SO_MAP_END:
		{
			if (! data_is_map_end(b))
				return compare_int64(*a, *b);
			data_read_map_end(&a);
			data_read_map_end(&b);
			level--;
			break;
		}
		case SO_INTERVAL:
		{
			if (! data_is_interval(b))
				return compare_int64(*a, *b);
			Interval a_iv;
			Interval b_iv;
			data_read_interval(&a, &a_iv);
			data_read_interval(&b, &b_iv);
			rc = interval_compare(&a_iv, &b_iv);
			if (rc != 0)
				return rc;
			break;
		}
		}

	} while (level > 0);

	return 0;
}
