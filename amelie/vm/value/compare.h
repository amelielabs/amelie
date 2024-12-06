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
	case TYPE_NULL:
		return 0;
	case TYPE_BOOL:
	case TYPE_INT:
	case TYPE_TIMESTAMP:
	{
		if (a->integer == b->integer)
			return 0;
		return (a->integer > b->integer) ? 1 : -1;
	}
	case TYPE_DOUBLE:
	{
		if (a->dbl == b->dbl)
			return 0;
		return (a->dbl > b->dbl) ? 1 : -1;
	}
	case TYPE_STRING:
		return str_compare_fn(&a->string, &b->string);
	case TYPE_JSON:
		return json_compare(a->json, b->json);
	case TYPE_INTERVAL:
		return interval_compare(&a->interval, &b->interval);
	case TYPE_VECTOR:
		return vector_compare(a->vector, b->vector);
	// TYPE_AVG
	// TYPE_STORE
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
	case TYPE_NULL:
		return false;
	case TYPE_INT:
	case TYPE_BOOL:
	case TYPE_TIMESTAMP:
		return a->integer > 0;
	case TYPE_DOUBLE:
		return a->dbl > 0.0;
	case TYPE_STRING:
		return !str_empty(&a->string);
	case TYPE_JSON:
		break;
	case TYPE_INTERVAL:
	case TYPE_VECTOR:
		return a->vector->size > 0;
	// TYPE_AVG
	// TYPE_STORE
	default:
		break;
	}
	return true;
}

always_inline hot static inline bool
value_is_unary(Value* result, Value* a)
{
	if (likely(a->type != TYPE_NULL))
		return true;
	value_free(a);
	value_set_null(result);
	return false;
}

always_inline hot static inline bool
value_is(Value* result, Value* a, Value* b)
{
	if (unlikely(a->type != TYPE_NULL && b->type != TYPE_NULL))
		return true;
	value_free(a);
	value_free(b);
	value_set_null(result);
	return false;
}
