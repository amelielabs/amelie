#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

always_inline hot static inline bool
value_is_true(Value* a)
{
	switch (a->type) {
	case VALUE_INT:
	case VALUE_BOOL:
		return a->integer > 0;
	case VALUE_REAL:
		return a->real > 0.0;
	case VALUE_NULL:
		return false;
	case VALUE_DATA:
	case VALUE_INTERVAL:
	case VALUE_TIMESTAMP:
	case VALUE_TIMESTAMPTZ:
	case VALUE_STRING:
	case VALUE_SET:
	case VALUE_MERGE:
	case VALUE_GROUP:
	case VALUE_NONE:
		break;
	}
	return true;
}

always_inline hot static inline bool
value_is_equal(Value* a, Value* b)
{
	switch (a->type) {
	case VALUE_INT:
	case VALUE_BOOL:
		if (b->type == VALUE_INT || b->type == VALUE_BOOL)
			return a->integer == b->integer;
		if (b->type == VALUE_REAL)
			return a->integer == b->real;
		break;
	case VALUE_REAL:
		if (b->type == VALUE_INT || b->type == VALUE_BOOL)
			return a->real == b->integer;
		if (b->type == VALUE_REAL)
			return a->real == b->real;
		break;
	case VALUE_NULL:
		return b->type == VALUE_NULL;
	case VALUE_DATA:
		if (b->type != VALUE_DATA || a->data_size != b->data_size)
			return false;
		return !memcmp(a->data, b->data, a->data_size);
	case VALUE_STRING:
		return str_compare(&a->string, &b->string);
	case VALUE_INTERVAL:
		if (b->type == VALUE_INTERVAL)
			return interval_compare(&a->interval, &b->interval) == 0;
		break;
	case VALUE_TIMESTAMP:
		if (b->type == VALUE_TIMESTAMP || b->type == VALUE_INT)
			return a->integer == b->integer;
		break;
	case VALUE_TIMESTAMPTZ:
		if (b->type == VALUE_TIMESTAMPTZ || b->type == VALUE_INT)
			return a->integer == b->integer;
		break;
	default:
		break;
	}
	error("bad = expression type");
	return false;
}

always_inline hot static inline void
value_bor(Value* result, Value* a, Value* b)
{
	if (unlikely(! (a->type == VALUE_INT && b->type == VALUE_INT)))
		error("bad | expression type");
	value_set_int(result, a->integer | b->integer);
}

always_inline hot static inline void
value_band(Value* result, Value* a, Value* b)
{
	if (unlikely(! (a->type == VALUE_INT && b->type == VALUE_INT)))
		error("bad & expression type");
	value_set_int(result, a->integer & b->integer);
}

always_inline hot static inline void
value_bxor(Value* result, Value* a, Value* b)
{
	if (unlikely(! (a->type == VALUE_INT && b->type == VALUE_INT)))
		error("bad ^ expression type");
	value_set_int(result, a->integer ^ b->integer);
}

always_inline hot static inline void
value_bshl(Value* result, Value* a, Value* b)
{
	if (unlikely(! (a->type == VALUE_INT && b->type == VALUE_INT)))
		error("bad << expression type");
	value_set_int(result, a->integer << b->integer);
}

always_inline hot static inline void
value_bshr(Value* result, Value* a, Value* b)
{
	if (unlikely(! (a->type == VALUE_INT && b->type == VALUE_INT)))
		error("bad << expression type");
	value_set_int(result, a->integer >> b->integer);
}

always_inline hot static inline void
value_not(Value* result, Value* val)
{
	value_set_bool(result, !value_is_true(val));
}

always_inline hot static inline void
value_equ(Value* result, Value* a, Value* b)
{
	value_set_bool(result, value_is_equal(a, b));
}

always_inline hot static inline void
value_nequ(Value* result, Value* a, Value* b)
{
	value_set_bool(result, !value_is_equal(a, b));
}

always_inline hot static inline void
value_gte(Value* result, Value* a, Value* b)
{
	if (a->type == VALUE_INT)
	{
		if (b->type == VALUE_INT)
			return value_set_bool(result, a->integer >= b->integer);
		if (b->type == VALUE_REAL)
			return value_set_bool(result, a->integer >= b->real);
	} else
	if (a->type == VALUE_REAL)
	{
		if (b->type == VALUE_INT)
			return value_set_bool(result, a->real >= b->integer);
		if (b->type == VALUE_REAL)
			return value_set_bool(result, a->real >= b->real);
	} else
	if (a->type == VALUE_INTERVAL)
	{
		if (b->type == VALUE_INTERVAL)
		{
			int rc = interval_compare(&a->interval, &b->interval);
			return value_set_bool(result, rc >= 0);
		}
	} else
	if (a->type == VALUE_TIMESTAMP)
	{
		if (b->type == VALUE_TIMESTAMP)
			return value_set_bool(result, a->integer >= b->integer);
	} else
	if (a->type == VALUE_TIMESTAMPTZ)
	{
		if (b->type == VALUE_TIMESTAMPTZ)
			return value_set_bool(result, a->integer >= b->integer);
	}
	error("bad >= expression type");
}

always_inline hot static inline void
value_gt(Value* result, Value* a, Value* b)
{
	if (a->type == VALUE_INT)
	{
		if (b->type == VALUE_INT)
			return value_set_bool(result, a->integer > b->integer);
		if (b->type == VALUE_REAL)
			return value_set_bool(result, a->integer > b->real);
	} else
	if (a->type == VALUE_REAL)
	{
		if (b->type == VALUE_INT)
			return value_set_bool(result, a->real > b->integer);
		if (b->type == VALUE_REAL)
			return value_set_bool(result, a->real > b->real);
	} else
	if (a->type == VALUE_INTERVAL)
	{
		if (b->type == VALUE_INTERVAL)
		{
			int rc = interval_compare(&a->interval, &b->interval);
			return value_set_bool(result, rc > 0);
		}
	} else
	if (a->type == VALUE_TIMESTAMP)
	{
		if (b->type == VALUE_TIMESTAMP)
			return value_set_bool(result, a->integer > b->integer);
	} else
	if (a->type == VALUE_TIMESTAMPTZ)
	{
		if (b->type == VALUE_TIMESTAMPTZ)
			return value_set_bool(result, a->integer > b->integer);
	}
	error("bad > expression type");
}

always_inline hot static inline void
value_lte(Value* result, Value* a, Value* b)
{
	if (a->type == VALUE_INT)
	{
		if (b->type == VALUE_INT)
			return value_set_bool(result, a->integer <= b->integer);
		if (b->type == VALUE_REAL)
			return value_set_bool(result, a->integer <= b->real);
	} else
	if (a->type == VALUE_REAL)
	{
		if (b->type == VALUE_INT)
			return value_set_bool(result, a->real <= b->integer);
		if (b->type == VALUE_REAL)
			return value_set_bool(result, a->real <= b->real);
	} else
	if (a->type == VALUE_INTERVAL)
	{
		if (b->type == VALUE_INTERVAL)
		{
			int rc = interval_compare(&a->interval, &b->interval);
			return value_set_bool(result, rc <= 0);
		}
	} else
	if (a->type == VALUE_TIMESTAMP)
	{
		if (b->type == VALUE_TIMESTAMP)
			return value_set_bool(result, a->integer <= b->integer);
	} else
	if (a->type == VALUE_TIMESTAMPTZ)
	{
		if (b->type == VALUE_TIMESTAMPTZ)
			return value_set_bool(result, a->integer <= b->integer);
	}
	error("bad <= expression type");
}

always_inline hot static inline void
value_lt(Value* result, Value* a, Value* b)
{
	if (a->type == VALUE_INT)
	{
		if (b->type == VALUE_INT)
			return value_set_bool(result, a->integer < b->integer);
		if (b->type == VALUE_REAL)
			return value_set_bool(result, a->integer < b->real);
	} else
	if (a->type == VALUE_REAL)
	{
		if (b->type == VALUE_INT)
			return value_set_bool(result, a->real < b->integer);
		if (b->type == VALUE_REAL)
			return value_set_bool(result, a->real < b->real);
	} else
	if (a->type == VALUE_INTERVAL)
	{
		if (b->type == VALUE_INTERVAL)
		{
			int rc = interval_compare(&a->interval, &b->interval);
			return value_set_bool(result, rc < 0);
		}
	} else
	if (a->type == VALUE_TIMESTAMP)
	{
		if (b->type == VALUE_TIMESTAMP)
			return value_set_bool(result, a->integer < b->integer);
	} else
	if (a->type == VALUE_TIMESTAMPTZ)
	{
		if (b->type == VALUE_TIMESTAMPTZ)
			return value_set_bool(result, a->integer < b->integer);
	}
	error("bad < expression type");
}

always_inline hot static inline void
value_add(Value* result, Value* a, Value* b)
{
	if (a->type == VALUE_INT)
	{
		if (b->type == VALUE_INT)
			return value_set_int(result, a->integer + b->integer);
		if (b->type == VALUE_REAL)
			return value_set_real(result, a->integer + b->real);
	} else
	if (a->type == VALUE_REAL)
	{
		if (b->type == VALUE_INT)
			return value_set_real(result, a->real + b->integer);
		if (b->type == VALUE_REAL)
			return value_set_real(result, a->real + b->real);
	} else
	if (a->type == VALUE_INTERVAL)
	{
		if (b->type == VALUE_INTERVAL)
		{
			Interval iv;
			interval_add(&iv, &a->interval, &b->interval);
			return value_set_interval(result, &iv);
		}
		if (b->type == VALUE_TIMESTAMP ||
		    b->type == VALUE_TIMESTAMPTZ)
		{
			Timestamp ts;
			timestamp_init(&ts);
			timestamp_read_value(&ts, b->integer);
			timestamp_add(&ts, &a->interval);
			if (b->type == VALUE_TIMESTAMPTZ)
				return value_set_timestamptz(result, timestamp_of(&ts, true));
			return value_set_timestamp(result, timestamp_of(&ts, false));
		}
	} else
	if (a->type == VALUE_TIMESTAMP ||
	    a->type == VALUE_TIMESTAMPTZ)
	{
		if (b->type == VALUE_INTERVAL)
		{
			Timestamp ts;
			timestamp_init(&ts);
			timestamp_read_value(&ts, a->integer);
			timestamp_add(&ts, &b->interval);
			if (a->type == VALUE_TIMESTAMPTZ)
				return value_set_timestamptz(result, timestamp_of(&ts, true));
			return value_set_timestamp(result, timestamp_of(&ts, false));
		}
	}
	error("bad + expression types");
}

always_inline hot static inline void
value_sub(Value* result, Value* a, Value* b)
{
	if (a->type == VALUE_INT)
	{
		if (b->type == VALUE_INT)
			return value_set_int(result, a->integer - b->integer);
		if (b->type == VALUE_REAL)
			return value_set_real(result, a->integer - b->real);
	} else
	if (a->type == VALUE_REAL)
	{
		if (b->type == VALUE_INT)
			return value_set_real(result, a->real - b->integer);
		if (b->type == VALUE_REAL)
			return value_set_real(result, a->real - b->real);
	} else
	if (a->type == VALUE_INTERVAL)
	{
		if (b->type == VALUE_INTERVAL)
		{
			Interval iv;
			interval_sub(&iv, &a->interval, &b->interval);
			return value_set_interval(result, &iv);
		}
		if (b->type == VALUE_TIMESTAMP ||
		    b->type == VALUE_TIMESTAMPTZ)
		{
			Timestamp ts;
			timestamp_init(&ts);
			timestamp_read_value(&ts, b->integer);
			timestamp_sub(&ts, &a->interval);
			if (b->type == VALUE_TIMESTAMPTZ)
				return value_set_timestamptz(result, timestamp_of(&ts, true));
			return value_set_timestamp(result, timestamp_of(&ts, false));
		}
	} else
	if (a->type == VALUE_TIMESTAMP ||
	    a->type == VALUE_TIMESTAMPTZ)
	{
		if (b->type == VALUE_INTERVAL)
		{
			Timestamp ts;
			timestamp_init(&ts);
			timestamp_read_value(&ts, a->integer);
			timestamp_sub(&ts, &b->interval);
			if (a->type == VALUE_TIMESTAMPTZ)
				return value_set_timestamptz(result, timestamp_of(&ts, true));
			return value_set_timestamp(result, timestamp_of(&ts, false));
		}
	}
	error("bad - expression types");
}

always_inline hot static inline void
value_mul(Value* result, Value* a, Value* b)
{
	if (a->type == VALUE_INT)
	{
		if (b->type == VALUE_INT)
			return value_set_int(result, a->integer * b->integer);
		if (b->type == VALUE_REAL)
			return value_set_real(result, a->integer * b->real);
	} else
	if (a->type == VALUE_REAL)
	{
		if (b->type == VALUE_INT)
			return value_set_real(result, a->real * b->integer);
		if (b->type == VALUE_REAL)
			return value_set_real(result, a->real * b->real);
	}
	error("bad * expression types");
}

always_inline hot static inline void
value_div(Value* result, Value* a, Value* b)
{
	if (a->type == VALUE_INT)
	{
		if (b->type == VALUE_INT)
		{
			if (unlikely(b->integer == 0))
				goto div_error;
			return value_set_int(result, a->integer / b->integer);
		}
		if (b->type == VALUE_REAL)
		{
			if (unlikely(b->real == 0.0))
				goto div_error;
			return value_set_real(result, a->integer / b->real);
		}
	} else
	if (a->type == VALUE_REAL)
	{
		if (b->type == VALUE_INT)
		{
			if (unlikely(b->integer == 0))
				goto div_error;
			return value_set_real(result, a->real / b->integer);
		}
		if (b->type == VALUE_REAL)
		{
			if (unlikely(b->real == 0.0))
				goto div_error;
			return value_set_real(result, a->real / b->real);
		}
	}
	error("bad / expression types");

div_error:
	error("zero division");
}

always_inline hot static inline void
value_mod(Value* result, Value* a, Value* b)
{
	if (! (a->type == VALUE_INT && b->type == VALUE_INT))
		error("bad %% expression types");
	if (unlikely(b->integer == 0))
		error("zero division");
	value_set_int(result, a->integer % b->integer);
}

always_inline hot static inline void
value_neg(Value* result, Value* a)
{
	if (a->type == VALUE_INT)
		return value_set_int(result, -a->integer);
	if (a->type == VALUE_REAL)
		return value_set_real(result, -a->real);
	error("bad - expression type");
}

always_inline hot static inline void
value_cat(Value* result, Value* a, Value* b)
{
	if (! (a->type == VALUE_STRING && b->type == VALUE_STRING))
		error("bad || expression types");

	auto buf = buf_begin();
	uint8_t** pos;
	pos = buf_reserve(buf, data_size_string(str_size(&a->string) + str_size(&b->string)));
	data_write_string_cat(pos, str_of(&a->string),
	                      str_of(&b->string),
	                      str_size(&a->string),
	                      str_size(&b->string));
	uint8_t* pos_str = buf->start;
	Str string;
	data_read_string(&pos_str, &string);
	buf_end(buf);

	value_set_string(result, &string, buf);
}

always_inline hot static inline void
value_sizeof(Value* result, Value* a)
{
	int rc = 1;
	switch (a->type) {
	case VALUE_NULL:
		rc = 0;
		break;
	case VALUE_DATA:
		if (data_is_array(a->data))
			rc = array_size(a->data);
		else
		if (data_is_map(a->data))
			rc = map_size(a->data);
		break;
	case VALUE_STRING:
		rc = str_size(&a->string);
		break;
	default:
		break;
	}
	value_set_int(result, rc);
}

always_inline hot static inline void
value_to_string(Value* result, Value* a)
{
	auto data = buf_begin();
	switch (a->type) {
	case VALUE_STRING:
		buf_printf(data, "%.*s", str_size(&a->string), str_of(&a->string));
		break;
	case VALUE_INTERVAL:
	{
		buf_reserve(data, 512);
		int size = interval_write(&a->interval, (char*)data->position, 512);
		buf_advance(data, size);
		break;

	case VALUE_TIMESTAMP:
	{
		buf_reserve(data, 128);
		int size = timestamp_write(a->integer, (char*)data->position, 128);
		buf_advance(data, size);
		break;
	}
	case VALUE_TIMESTAMPTZ:
	{
		// TODO: use tz convertion
		buf_reserve(data, 128);
		int size = timestamp_write(a->integer, (char*)data->position, 128);
		buf_advance(data, size);
		break;
	}
	}
	default:
		body_add(data, a, false);
		break;
	}
	buf_end(data);

	Str string;
	str_init(&string);
	str_set(&string, (char*)data->start, buf_size(data));
	value_set_string(result, &string, data);
}

always_inline hot static inline void
value_to_json(Value* result, Value* a)
{
	if (unlikely(a->type != VALUE_STRING))
		error("json(): string type expected");

	auto buf = buf_begin();
	Json json;
	json_init(&json);
	guard(json_free, &json);
	json_parse(&json, &a->string, buf);
	buf_end(buf);
	value_set_buf(result, buf);
}

always_inline hot static inline void
value_assign(Value* result, int column, Value* a, Value* b, Value* c)
{
	if (unlikely(a->type != VALUE_DATA))
		error("set(): map or array type expected");

	if (unlikely(! data_is_array(a->data)))
		error("set(): array type expected");

	if (b->type == VALUE_NULL)
	{
		// row[column] = value
		value_array_set(result, a->data, column, c);
	} else
	if (b->type == VALUE_STRING)
	{
		// row[column].path = value
		update_set_array(result, a->data, column, &b->string, c);
	} else
	{
		error("set(): path type must be string");
	}
}

always_inline hot static inline void
value_idx_set(Value* result, Value* a, Value* b, Value* c)
{
	if (unlikely(a->type == VALUE_NULL))
	{
		if (unlikely(b->type != VALUE_STRING))
			error("set(): path type must be string");
		value_map_as(result, b, c);
		return;
	}
	if (unlikely(a->type != VALUE_DATA))
		error("set(): map or array type expected");
	if (data_is_map(a->data))
	{
		if (unlikely(b->type != VALUE_STRING))
			error("set(): path type must be string");
		update_set(result, a->data, &b->string, c);
	} else
	if (data_is_array(a->data))
	{
		if (b->type == VALUE_INT)
			value_array_set(result, a->data, b->integer, c);
		else
			error("set(): position type must be integer");
	}
}

always_inline hot static inline void
value_idx_unset(Value* result, Value* a, Value* b)
{
	if (unlikely(a->type != VALUE_DATA))
		error("unset(): map or array type expected");
	if (data_is_map(a->data))
	{
		if (unlikely(b->type != VALUE_STRING))
			error("unset(): path type must be string");
		update_unset(result, a->data, &b->string);
	} else
	if (data_is_array(a->data))
	{
		if (b->type == VALUE_INT)
			value_array_remove(result, a->data, b->integer);
		else
			error("unset(): map or array type expected");
	} else
	{
		error("unset(): map or array type expected");
	}
}

always_inline hot static inline void
value_idx_has(Value* result, Value* a, Value* b)
{
	if (unlikely(a->type != VALUE_DATA))
		error("has(): map or array type expected");
	if (data_is_map(a->data))
	{
		if (unlikely(b->type != VALUE_STRING))
			error("has(): path type must be string");
		value_map_has(result, a->data, &b->string);
	} else
	if (data_is_array(a->data))
	{
		if (unlikely(b->type != VALUE_INT))
			error("has(): position type must be integer");
		value_array_has(result, a->data, b->integer);
	} else
	{
		error("has(): map or array type expected");
	}
}

always_inline hot static inline void
value_idx(Value* result, Value* a, Value* b)
{
	if (unlikely(a->type != VALUE_DATA))
		error("[]: map or array type expected");

	if (data_is_map(a->data))
	{
		// map['path']
		if (unlikely(b->type != VALUE_STRING))
			error("[]: map key type must be string");
		uint8_t* data = a->data;
		if (! map_find_path(&data, &b->string))
			error("<%.*s>: map path not found", str_size(&b->string),
				  str_of(&b->string));
		value_read(result, data, a->buf);
	} else
	if (data_is_array(a->data))
	{
		/* array[idx] */
		if (unlikely(b->type != VALUE_INT))
			error("[]: array key type must be int");
		uint8_t* data = a->data;
		if (! array_find(&data, b->integer))
			error("<%d>: array index not found", b->integer);
		value_read(result, data, a->buf);
	} else
	{
		error("[]: map or array type expected");
	}
}

always_inline hot static inline void
value_append(Value* result, Value* a, Value* b)
{
	uint8_t* data;
	int      data_size;
	if (a->type == VALUE_DATA)
	{
		data = a->data;
		data_size = a->data_size;
		if (! data_is_array(data))
			error("append(): array or null expected");
	} else
	if (a->type == VALUE_NULL)
	{
		data = NULL;
		data_size = 0;
	} else
	{
		error("append(): array or null expected");
	}
	value_array_append(result, data, data_size, b);
}
