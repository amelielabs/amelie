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

always_inline hot static inline bool
value_nullu_fast(Value* result, Value* a)
{
	if (likely(a->type != TYPE_NULL))
		return true;
	value_set_null(result);
	return false;
}

always_inline hot static inline bool
value_nullu(Value* result, Value* a)
{
	if (likely(a->type != TYPE_NULL))
		return true;
	value_free(a);
	value_set_null(result);
	return false;
}

always_inline hot static inline bool
value_null_fast(Value* result, Value* a, Value* b)
{
	if (likely(a->type != TYPE_NULL && b->type != TYPE_NULL))
		return true;
	value_set_null(result);
	return false;
}

always_inline hot static inline bool
value_null(Value* result, Value* a, Value* b)
{
	if (likely(a->type != TYPE_NULL && b->type != TYPE_NULL))
		return true;
	value_free(a);
	value_free(b);
	value_set_null(result);
	return false;
}
