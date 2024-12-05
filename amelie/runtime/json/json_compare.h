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

always_inline hot static inline int
json_compare_integer_read(uint8_t** a, uint8_t** b)
{
	int64_t a_value;
	int64_t b_value;
	json_read_integer(a, &a_value);
	json_read_integer(b, &b_value);
	return compare_int64(a_value, b_value);
}

always_inline hot static inline int
json_compare_integer(uint8_t* a, uint8_t* b)
{
	return json_compare_integer_read(&a, &b);
}

always_inline hot static inline int
json_compare_string_read(uint8_t** a, uint8_t** b)
{
	char* a_value;
	int   a_value_size;
	char* b_value;
	int   b_value_size;
	json_read_raw(a, &a_value, &a_value_size);
	json_read_raw(b, &b_value, &b_value_size);
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
json_compare_string(uint8_t* a, uint8_t* b)
{
	return json_compare_string_read(&a, &b);
}

hot static inline int
json_compare(uint8_t* a, uint8_t* b)
{
	int level = 0;
	int rc;
	do
	{
		switch (*a) {
		case JSON_TRUE:
		case JSON_FALSE:
		{
			if (! json_is_bool(b))
				return compare_int64(*a, *b);
			bool a_value;
			bool b_value;
			json_read_bool(&a, &a_value);
			json_read_bool(&b, &b_value);
			rc = compare_int64(a_value, b_value);
			if (rc != 0)
				return rc;
			break;
		}
		case JSON_NULL:
		{
			if (! json_is_null(b))
				return compare_int64(*a, *b);
			json_skip(&a);
			json_skip(&b);
			break;
		}
		case JSON_REAL32:
		case JSON_REAL64:
		{
			if (! json_is_real(b))
				return compare_int64(*a, *b);
			double a_value;
			double b_value;
			json_read_real(&a, &a_value);
			json_read_real(&b, &b_value);
			if (a_value == b_value)
				break;
			return (a_value > b_value) ? 1 : -1;
		}
		case JSON_INTV0 ... JSON_INT64:
		{
			if (! json_is_integer(b))
				return compare_int64(*a, *b);
			rc = json_compare_integer_read(&a, &b);
			if (rc != 0)
				return rc;
			break;
		}
		case JSON_STRINGV0 ... JSON_STRING32:
		{
			if (! json_is_string(b))
				return compare_int64(*a, *b);
			rc = json_compare_string_read(&a, &b);
			if (rc != 0)
				return rc;
			break;
		}
		case JSON_ARRAY:
		{
			if (! json_is_array(b))
				return compare_int64(*a, *b);
			json_read_array(&a);
			json_read_array(&b);
			level++;
			break;
		}
		case JSON_ARRAY_END:
		{
			if (! json_is_array_end(b))
				return compare_int64(*a, *b);
			json_read_array_end(&a);
			json_read_array_end(&b);
			level--;
			break;
		}
		case JSON_OBJ:
		{
			if (! json_is_obj(b))
				return compare_int64(*a, *b);
			json_read_obj(&a);
			json_read_obj(&b);
			level++;
			break;
		}
		case JSON_OBJ_END:
		{
			if (! json_is_obj_end(b))
				return compare_int64(*a, *b);
			json_read_obj_end(&a);
			json_read_obj_end(&b);
			level--;
			break;
		}
		}

	} while (level > 0);

	return 0;
}
