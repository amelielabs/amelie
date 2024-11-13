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
value_compare_int(Value* a, Value* b)
{
	if (a->integer == b->integer)
		return 0;
	return (a->integer > b->integer) ? 1 : -1;
}

always_inline hot static inline int
value_compare_string(Value* a, Value* b)
{
	return str_compare_fn(&a->string, &b->string);
}

always_inline hot static inline int
value_compare(Value* a, Value* b)
{
	if (a->type != b->type)
		return (a->type > b->type) ? 1 : -1;

	switch (a->type) {
	case VALUE_NULL:
		return 0;
	case VALUE_BOOL:
	case VALUE_INT:
	case VALUE_TIMESTAMP:
	{
		if (a->integer == b->integer)
			return 0;
		return (a->integer > b->integer) ? 1 : -1;
	}
	case VALUE_DOUBLE:
	{
		if (a->dbl == b->dbl)
			return 0;
		return (a->dbl > b->dbl) ? 1 : -1;
	}
	case VALUE_INTERVAL:
		return interval_compare(&a->interval, &b->interval);
	case VALUE_STRING:
		return str_compare_fn(&a->string, &b->string);
	case VALUE_JSON:
		return data_compare(a->data, b->data);
	case VALUE_VECTOR:
		return vector_compare(a->vector, b->vector);
	// VALUE_AVG
	// VALUE_SET
	// VALUE_MERGE
	default:
		break;
	}

	error("unsupported operation");
	return -1;
}

always_inline hot static inline bool
value_is_true(Value* a)
{
	switch (a->type) {
	case VALUE_NULL:
		return false;
	case VALUE_INT:
	case VALUE_BOOL:
	case VALUE_TIMESTAMP:
		return a->integer > 0;
	case VALUE_DOUBLE:
		return a->dbl > 0.0;
	case VALUE_INTERVAL:
	case VALUE_STRING:
		return !str_empty(&a->string);
	case VALUE_JSON:
		break;
	case VALUE_VECTOR:
		return a->vector->size > 0;
	// VALUE_AVG
	// VALUE_SET
	// VALUE_MERGE
	default:
		break;
	}
	return true;
}

always_inline hot static inline bool
value_is_unary(Value* result, Value* a)
{
	if (likely(a->type != VALUE_NULL))
		return true;
	value_free(a);
	value_set_null(result);
	return false;
}

always_inline hot static inline bool
value_is(Value* result, Value* a, Value* b)
{
	if (unlikely(a->type != VALUE_NULL && b->type != VALUE_NULL))
		return true;
	value_free(a);
	value_free(b);
	value_set_null(result);
	return false;
}
