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
data_compare_int_read(uint8_t** a, uint8_t** b)
{
	int64_t a_value;
	int64_t b_value;
	unpack_int(a, &a_value);
	unpack_int(b, &b_value);
	return compare_int64(a_value, b_value);
}

always_inline hot static inline int
data_compare_int(uint8_t* a, uint8_t* b)
{
	return data_compare_int_read(&a, &b);
}

always_inline hot static inline int
data_compare_str_read(uint8_t** a, uint8_t** b)
{
	Str str_a;
	Str str_b;
	unpack_str(a, &str_a);
	unpack_str(b, &str_b);
	return str_compare_fn(&str_a, &str_b);
}

always_inline hot static inline int
data_compare_str(uint8_t* a, uint8_t* b)
{
	return data_compare_str_read(&a, &b);
}

hot static inline int
data_compare(uint8_t* a, uint8_t* b)
{
	int level = 0;
	int rc;
	do
	{
		switch (*a) {
		case DATA_TRUE:
		case DATA_FALSE:
		{
			if (! data_is_bool(b))
				return compare_int64(*a, *b);
			bool a_value;
			bool b_value;
			unpack_bool(&a, &a_value);
			unpack_bool(&b, &b_value);
			rc = compare_int64(a_value, b_value);
			if (rc != 0)
				return rc;
			break;
		}
		case DATA_NULL:
		{
			if (! data_is_null(b))
				return compare_int64(*a, *b);
			data_skip(&a);
			data_skip(&b);
			break;
		}
		case DATA_REAL32:
		case DATA_REAL64:
		{
			if (! data_is_real(b))
				return compare_int64(*a, *b);
			double a_value;
			double b_value;
			unpack_real(&a, &a_value);
			unpack_real(&b, &b_value);
			rc = compare_double(a_value, b_value);
			if (rc != 0)
				return rc;
			break;
		}
		case DATA_INTV0 ... DATA_INT64:
		{
			if (! data_is_int(b))
				return compare_int64(*a, *b);
			rc = data_compare_int_read(&a, &b);
			if (rc != 0)
				return rc;
			break;
		}
		case DATA_STRV0 ... DATA_STR32:
		{
			if (! data_is_str(b))
				return compare_int64(*a, *b);
			rc = data_compare_str_read(&a, &b);
			if (rc != 0)
				return rc;
			break;
		}
		case DATA_ARRAY:
		{
			if (! data_is_array(b))
				return compare_int64(*a, *b);
			unpack_array(&a);
			unpack_array(&b);
			level++;
			break;
		}
		case DATA_ARRAY_END:
		{
			if (! data_is_array_end(b))
				return compare_int64(*a, *b);
			unpack_array_end(&a);
			unpack_array_end(&b);
			level--;
			break;
		}
		case DATA_OBJ:
		{
			if (! data_is_obj(b))
				return compare_int64(*a, *b);
			unpack_obj(&a);
			unpack_obj(&b);
			level++;
			break;
		}
		case DATA_OBJ_END:
		{
			if (! data_is_obj_end(b))
				return compare_int64(*a, *b);
			unpack_obj_end(&a);
			unpack_obj_end(&b);
			level--;
			break;
		}
		}

	} while (level > 0);

	return 0;
}
