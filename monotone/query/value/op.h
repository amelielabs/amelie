#pragma once

//
// monotone
//
// SQL OLTP database
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

	auto buf = msg_create(MSG_OBJECT);
	uint8_t** pos;
	pos = buf_reserve(buf, data_size_string(str_size(&a->string) + str_size(&b->string)));
	data_write_string_cat(pos, str_of(&a->string),
	                      str_of(&b->string),
	                      str_size(&a->string),
	                      str_size(&b->string));
	msg_end(buf);

	uint8_t* pos_str = msg_of(buf)->data;
	Str string;
	data_read_string(&pos_str, &string);
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
			rc = data_read_array_at(a->data);
		else
		if (data_is_map(a->data))
			rc = data_read_map_at(a->data);
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
	// todo: check type
	if (unlikely(a->type != VALUE_DATA))
		error("to_string(): array/map type expected");

	auto data = buf_create(0);
	value_write(a, data);
	auto data_pos = data->start;
	auto buf = buf_create(0);
	json_to_string(buf, &data_pos);

	Str string;
	str_init(&string);
	str_set(&string, (char*)buf->start, buf_size(buf));
	value_set_string(result, &string, buf);
	buf_free(data);
}

always_inline hot static inline void
value_to_json(Value* result, Value* a)
{
	if (unlikely(a->type != VALUE_STRING))
		error("json(): string type expected");
	Json json;
	json_init(&json);
	guard(guard, json_free, &json);
	json_parse(&json, &a->string);
	auto data = make_copy_buf(json.buf);
	value_set_data_from(result, data);
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

