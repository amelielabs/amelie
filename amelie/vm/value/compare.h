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
	case VALUE_INT:
	case VALUE_BOOL:
	case VALUE_TIMESTAMP:
	{
		if (a->integer == b->integer)
			return 0;
		return (a->integer > b->integer) ? 1 : -1;
	}
	case VALUE_REAL:
	{
		if (a->real == b->real)
			return 0;
		return (a->real > b->real) ? 1 : -1;
	}
	case VALUE_NULL:
		return 0;
	case VALUE_STRING:
		return str_compare_fn(&a->string, &b->string);
	case VALUE_OBJ:
	case VALUE_ARRAY:
		return data_compare(a->data, b->data);
	case VALUE_INTERVAL:
		return interval_compare(&a->interval, &b->interval);
	// VALUE_NONE:
	// VALUE_SET:
	// VALUE_MERGE:
	// VALUE_GROUP:
	default:
		break;
	}

	error("unsupported operation");
	return -1;
}
