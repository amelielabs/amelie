#pragma once

//
// indigo
//
// SQL OLTP database
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
	case VALUE_DATA:
		return data_compare(a->data, b->data);

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
