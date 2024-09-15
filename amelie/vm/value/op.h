#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

always_inline hot static inline bool
value_is_true(Value* a)
{
	switch (a->type) {
	case VALUE_INT:
	case VALUE_BOOL:
	case VALUE_TIMESTAMP:
		return a->integer > 0;
	case VALUE_REAL:
		return a->real > 0.0;
	case VALUE_NULL:
		return false;
	case VALUE_MAP:
	case VALUE_ARRAY:
	case VALUE_INTERVAL:
	case VALUE_VECTOR:
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
		if (b->type == VALUE_INT || b->type == VALUE_BOOL ||
		    b->type == VALUE_TIMESTAMP)
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
	case VALUE_MAP:
	case VALUE_ARRAY:
		if (b->type != a->type || a->data_size != b->data_size)
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
	case VALUE_VECTOR:
		if (b->type == VALUE_VECTOR)
			return vector_compare(&a->vector, &b->vector) == 0;
		break;
	default:
		break;
	}
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
	switch (a->type) {
	case VALUE_INT:
		if (b->type == VALUE_INT || b->type == VALUE_TIMESTAMP)
			return value_set_bool(result, a->integer >= b->integer);
		if (b->type == VALUE_REAL)
			return value_set_bool(result, a->integer >= b->real);
		break;
	case VALUE_REAL:
		if (b->type == VALUE_INT)
			return value_set_bool(result, a->real >= b->integer);
		if (b->type == VALUE_REAL)
			return value_set_bool(result, a->real >= b->real);
		break;
	case VALUE_STRING:
		if (b->type == VALUE_STRING)
		{
			int rc = str_compare_fn(&a->string, &b->string);
			return value_set_bool(result, rc >= 0);
		}
		break;
	case VALUE_INTERVAL:
		if (b->type == VALUE_INTERVAL)
		{
			int rc = interval_compare(&a->interval, &b->interval);
			return value_set_bool(result, rc >= 0);
		}
		break;
	case VALUE_TIMESTAMP:
		if (b->type == VALUE_TIMESTAMP || b->type == VALUE_INT)
			return value_set_bool(result, a->integer >= b->integer);
		break;
	case VALUE_VECTOR:
		if (b->type == VALUE_VECTOR)
		{
			int rc = vector_compare(&a->vector, &b->vector);
			return value_set_bool(result, rc >= 0);
		}
		break;
	default:
		break;
	}
	error("bad >= expression type");
}

always_inline hot static inline void
value_gt(Value* result, Value* a, Value* b)
{
	switch (a->type) {
	case VALUE_INT:
		if (b->type == VALUE_INT || b->type == VALUE_TIMESTAMP)
			return value_set_bool(result, a->integer > b->integer);
		if (b->type == VALUE_REAL)
			return value_set_bool(result, a->integer > b->real);
		break;
	case VALUE_REAL:
		if (b->type == VALUE_INT)
			return value_set_bool(result, a->real > b->integer);
		if (b->type == VALUE_REAL)
			return value_set_bool(result, a->real > b->real);
		break;
	case VALUE_STRING:
		if (b->type == VALUE_STRING)
		{
			int rc = str_compare_fn(&a->string, &b->string);
			return value_set_bool(result, rc > 0);
		}
		break;
	case VALUE_INTERVAL:
		if (b->type == VALUE_INTERVAL)
		{
			int rc = interval_compare(&a->interval, &b->interval);
			return value_set_bool(result, rc > 0);
		}
		break;
	case VALUE_TIMESTAMP:
		if (b->type == VALUE_TIMESTAMP || b->type == VALUE_INT)
			return value_set_bool(result, a->integer > b->integer);
		break;
	case VALUE_VECTOR:
		if (b->type == VALUE_VECTOR)
		{
			int rc = vector_compare(&a->vector, &b->vector);
			return value_set_bool(result, rc > 0);
		}
		break;
	default:
		break;
	}
	error("bad > expression type");
}

always_inline hot static inline void
value_lte(Value* result, Value* a, Value* b)
{
	switch (a->type) {
	case VALUE_INT:
		if (b->type == VALUE_INT || b->type == VALUE_TIMESTAMP)
			return value_set_bool(result, a->integer <= b->integer);
		if (b->type == VALUE_REAL)
			return value_set_bool(result, a->integer <= b->real);
		break;
	case VALUE_REAL:
		if (b->type == VALUE_INT)
			return value_set_bool(result, a->real <= b->integer);
		if (b->type == VALUE_REAL)
			return value_set_bool(result, a->real <= b->real);
		break;
	case VALUE_STRING:
		if (b->type == VALUE_STRING)
		{
			int rc = str_compare_fn(&a->string, &b->string);
			return value_set_bool(result, rc <= 0);
		}
		break;
	case VALUE_INTERVAL:
		if (b->type == VALUE_INTERVAL)
		{
			int rc = interval_compare(&a->interval, &b->interval);
			return value_set_bool(result, rc <= 0);
		}
		break;
	case VALUE_TIMESTAMP:
		if (b->type == VALUE_TIMESTAMP || b->type == VALUE_INT)
			return value_set_bool(result, a->integer <= b->integer);
		break;
	case VALUE_VECTOR:
		if (b->type == VALUE_VECTOR)
		{
			int rc = vector_compare(&a->vector, &b->vector);
			return value_set_bool(result, rc <= 0);
		}
		break;
	default:
		break;
	}
	error("bad <= expression type");
}

always_inline hot static inline void
value_lt(Value* result, Value* a, Value* b)
{
	switch (a->type) {
	case VALUE_INT:
		if (b->type == VALUE_INT || b->type == VALUE_TIMESTAMP)
			return value_set_bool(result, a->integer < b->integer);
		if (b->type == VALUE_REAL)
			return value_set_bool(result, a->integer < b->real);
		break;
	case VALUE_REAL:
		if (b->type == VALUE_INT)
			return value_set_bool(result, a->real < b->integer);
		if (b->type == VALUE_REAL)
			return value_set_bool(result, a->real < b->real);
		break;
	case VALUE_STRING:
		if (b->type == VALUE_STRING)
		{
			int rc = str_compare_fn(&a->string, &b->string);
			return value_set_bool(result, rc < 0);
		}
		break;
	case VALUE_INTERVAL:
		if (b->type == VALUE_INTERVAL)
		{
			int rc = interval_compare(&a->interval, &b->interval);
			return value_set_bool(result, rc < 0);
		}
		break;
	case VALUE_TIMESTAMP:
		if (b->type == VALUE_TIMESTAMP || b->type == VALUE_INT)
			return value_set_bool(result, a->integer < b->integer);
		break;
	case VALUE_VECTOR:
		if (b->type == VALUE_VECTOR)
		{
			int rc = vector_compare(&a->vector, &b->vector);
			return value_set_bool(result, rc < 0);
		}
		break;
	default:
		break;
	}
	error("bad < expression type");
}

always_inline hot static inline void
value_in(Value* result, Value* a, Value* b)
{
	bool in = false;
	if (b->type == VALUE_SET)
		in = b->obj->in(b->obj, a);
	else
		in = value_is_equal(a, b);
	return value_set_bool(result, in);
}

always_inline hot static inline void
value_add(Value* result, Value* a, Value* b)
{
	switch (a->type) {
	case VALUE_INT:
		if (b->type == VALUE_INT)
			return value_set_int(result, a->integer + b->integer);
		if (b->type == VALUE_REAL)
			return value_set_real(result, a->integer + b->real);
		break;
	case VALUE_REAL:
		if (b->type == VALUE_INT)
			return value_set_real(result, a->real + b->integer);
		if (b->type == VALUE_REAL)
			return value_set_real(result, a->real + b->real);
		break;
	case VALUE_INTERVAL:
		if (b->type == VALUE_INTERVAL)
		{
			Interval iv;
			interval_add(&iv, &a->interval, &b->interval);
			return value_set_interval(result, &iv);
		}
		if (b->type == VALUE_TIMESTAMP)
		{
			Timestamp ts;
			timestamp_init(&ts);
			timestamp_read_value(&ts, b->integer);
			timestamp_add(&ts, &a->interval);
			return value_set_timestamp(result, timestamp_of(&ts, NULL));
		}
		break;
	case VALUE_TIMESTAMP:
		if (b->type == VALUE_INTERVAL)
		{
			Timestamp ts;
			timestamp_init(&ts);
			timestamp_read_value(&ts, a->integer);
			timestamp_add(&ts, &b->interval);
			return value_set_timestamp(result, timestamp_of(&ts, NULL));
		}
		break;
	case VALUE_VECTOR:
		if (b->type == VALUE_VECTOR)
		{
			if (a->vector.size != b->vector.size)
				error("vector sizes mismatch");
			Vector vector;
			auto buf = vector_create(&vector, a->vector.size);
			vector_add(&vector, &a->vector, &b->vector);
			return value_set_vector(result, &vector, buf);
		}
		break;
	default:
		break;
	}
	error("bad + expression types");
}

always_inline hot static inline void
value_sub(Value* result, Value* a, Value* b)
{
	switch (a->type) {
	case VALUE_INT:
		if (b->type == VALUE_INT)
			return value_set_int(result, a->integer - b->integer);
		if (b->type == VALUE_REAL)
			return value_set_real(result, a->integer - b->real);
		break;
	case VALUE_REAL:
		if (b->type == VALUE_INT)
			return value_set_real(result, a->real - b->integer);
		if (b->type == VALUE_REAL)
			return value_set_real(result, a->real - b->real);
		break;
	case VALUE_INTERVAL:
		if (b->type == VALUE_INTERVAL)
		{
			Interval iv;
			interval_sub(&iv, &a->interval, &b->interval);
			return value_set_interval(result, &iv);
		}
		if (b->type == VALUE_TIMESTAMP)
		{
			Timestamp ts;
			timestamp_init(&ts);
			timestamp_read_value(&ts, b->integer);
			timestamp_sub(&ts, &a->interval);
			return value_set_timestamp(result, timestamp_of(&ts, NULL));
		}
		break;
	case VALUE_TIMESTAMP:
		if (b->type == VALUE_INTERVAL)
		{
			Timestamp ts;
			timestamp_init(&ts);
			timestamp_read_value(&ts, a->integer);
			timestamp_sub(&ts, &b->interval);
			return value_set_timestamp(result, timestamp_of(&ts, NULL));
		}
		break;
	case VALUE_VECTOR:
		if (b->type == VALUE_VECTOR)
		{
			if (a->vector.size != b->vector.size)
				error("vector sizes mismatch");
			Vector vector;
			auto buf = vector_create(&vector, a->vector.size);
			vector_sub(&vector, &a->vector, &b->vector);
			return value_set_vector(result, &vector, buf);
		}
		break;
	default:
		break;
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
	} else
	if (a->type == VALUE_VECTOR)
	{
		if (b->type == VALUE_VECTOR)
		{
			if (a->vector.size != b->vector.size)
				error("vector sizes mismatch");
			Vector vector;
			auto buf = vector_create(&vector, a->vector.size);
			vector_mul(&vector, &a->vector, &b->vector);
			return value_set_vector(result, &vector, buf);
		}
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

	auto buf = buf_create();
	uint8_t** pos;
	pos = buf_reserve(buf, data_size_string(str_size(&a->string) + str_size(&b->string)));
	data_write_string_cat(pos, str_of(&a->string),
	                      str_of(&b->string),
	                      str_size(&a->string),
	                      str_size(&b->string));
	uint8_t* pos_str = buf->start;
	Str string;
	data_read_string(&pos_str, &string);
	value_set_string(result, &string, buf);
}

always_inline hot static inline void
value_length(Value* result, Value* a)
{
	int64_t rc = 0;
	switch (a->type) {
	case VALUE_NULL:
		break;
	case VALUE_MAP:
		rc = map_size(a->data);
		break;
	case VALUE_ARRAY:
		rc = array_size(a->data);
		break;
	case VALUE_STRING:
		rc = str_size(&a->string);
		break;
	case VALUE_VECTOR:
		rc = a->vector.size;
		break;
	default:
		error("length(): operation type is not supported");
		break;
	}
	value_set_int(result, rc);
}

always_inline hot static inline void
value_to_string(Value* result, Value* a, Timezone* timezone)
{
	auto data = buf_create();
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
		int size = timestamp_write(a->integer, timezone, (char*)data->position, 128);
		buf_advance(data, size);
		break;
	}
	}
	default:
		body_add(data, a, timezone, false, false);
		break;
	}

	Str string;
	str_init(&string);
	str_set(&string, (char*)data->start, buf_size(data));
	value_set_string(result, &string, data);
}

always_inline hot static inline void
value_to_int(Value* result, Value* a)
{
	int64_t value = 0;
	switch (a->type) {
	case VALUE_REAL:
		value = a->real;
		break;
	case VALUE_BOOL:
	case VALUE_INT:
	case VALUE_TIMESTAMP:
		value = a->integer;
		break;
	case VALUE_STRING:
		if (str_toint(&a->string, &value) == -1)
			error("int(): failed to cast string");
		break;
	default:
		error("int(): operation type is not supported");
		break;
	}
	value_set_int(result, value);
}

always_inline hot static inline void
value_to_bool(Value* result, Value* a)
{
	bool value = false;
	switch (a->type) {
	case VALUE_REAL:
		value = a->real > 0.0;
		break;
	case VALUE_BOOL:
	case VALUE_INT:
	case VALUE_TIMESTAMP:
		value = a->integer > 0;
		break;
	case VALUE_INTERVAL:
		value = (a->interval.us + a->interval.d + a->interval.m) > 0;
		break;
	default:
		error("bool(): operation type is not supported");
		break;
	}
	value_set_int(result, value);
}

always_inline hot static inline void
value_to_real(Value* result, Value* a)
{
	double value = 0;
	switch (a->type) {
	case VALUE_REAL:
		value = a->real;
		break;
	case VALUE_BOOL:
	case VALUE_INT:
	case VALUE_TIMESTAMP:
		value = a->integer;
		break;
	default:
		error("real(): operation type is not supported");
		break;
	}
	value_set_real(result, value);
}

always_inline hot static inline void
value_to_native(Value* result, Value* a, Local* local)
{
	if (unlikely(a->type != VALUE_STRING))
		error("native(): string type expected");

	auto buf = buf_create();
	guard_buf(buf);

	Json json;
	json_init(&json);
	guard(json_free, &json);
	json_set_time(&json, local->timezone, local->time_us);
	json_parse(&json, &a->string, buf);

	value_read(result, buf->start, buf);
	if (! result->buf)
		buf_free(buf);

	unguard();
	unguard();
	json_free(&json);
}

always_inline hot static inline void
value_assign(Value* result, int column, Value* a, Value* b, Value* c)
{
	if (unlikely(a->type != VALUE_ARRAY))
		error("set: row expected");

	if (b->type == VALUE_NULL)
	{
		// row[column] = value
		value_array_put(result, a->data, column, c);
	} else
	if (b->type == VALUE_STRING)
	{
		// row[column].path = value
		update_set_array(result, a->data, column, &b->string, c);
	} else {
		error("set: path type must be string");
	}
}

always_inline hot static inline void
value_set(Value* result, Value* a, Value* b, Value* c)
{
	if (unlikely(a->type != VALUE_MAP))
		error("set(): map expected");
	if (unlikely(b->type != VALUE_STRING))
		error("set(): path type must be string");
	update_set(result, a->data, &b->string, c);
}

always_inline hot static inline void
value_unset(Value* result, Value* a, Value* b)
{
	if (unlikely(a->type != VALUE_MAP))
		error("unset(): map expected");
	if (unlikely(b->type != VALUE_STRING))
		error("unset(): path type must be string");
	update_unset(result, a->data, &b->string);
}

always_inline hot static inline void
value_has(Value* result, Value* a, Value* b)
{
	if (unlikely(a->type != VALUE_MAP))
		error("has(): map expected");
	if (unlikely(b->type != VALUE_STRING))
		error("has(): path type must be string");
	value_map_has(result, a->data, &b->string);
}

always_inline hot static inline void
value_idx(Value* result, Value* a, Value* b)
{
	switch (a->type) {
	case VALUE_VECTOR:
	{
		// vector[idx]
		if (unlikely(b->type != VALUE_INT))
			error("[]: vector key type must be int");
		if (unlikely(b->integer < 0 || b->integer >= a->vector.size))
			error("[]: vector index  is out of bounds");
		value_set_real(result, a->vector.value[b->integer]);
		break;
	}
	case VALUE_MAP:
	{
		// map['path']
		if (unlikely(b->type != VALUE_STRING))
			error("[]: map key type must be string");
		uint8_t* data = a->data;
		if (! map_find_path(&data, &b->string))
			error("<%.*s>: map path not found", str_size(&b->string),
				  str_of(&b->string));
		value_read_ref(result, data, a->buf);
		break;
	}
	case VALUE_ARRAY:
	{
		// array[idx]
		if (unlikely(b->type != VALUE_INT))
			error("[]: array key type must be int");
		uint8_t* data = a->data;
		if (! array_find(&data, b->integer))
			error("<%d>: array index not found", b->integer);
		value_read_ref(result, data, a->buf);
		break;
	}
	default:
		error("[]: map or array expected");
		break;
	}
}

always_inline hot static inline void
value_append(Value* result, Value* a, int argc, Value** argv)
{
	if (unlikely(a->type != VALUE_ARRAY))
		error("append(): array expected");
	value_array_append(result, a->data, a->data_size, argc, argv);
}
