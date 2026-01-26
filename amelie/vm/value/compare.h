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
value_is_true(Value* a)
{
	switch (a->type) {
	case TYPE_NULL:
		return false;
	case TYPE_INT:
	case TYPE_BOOL:
	case TYPE_DATE:
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
	// TYPE_REF
	// TYPE_STORE
	// TYPE_CURSOR
	// TYPE_CURSOR_STORE
	// TYPE_CURSOR_JSON
	default:
		break;
	}
	return true;
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
	case TYPE_DATE:
	case TYPE_TIMESTAMP:
		return compare_int64(a->integer, b->integer);
	case TYPE_DOUBLE:
		return compare_double(a->dbl, b->dbl);
	case TYPE_STRING:
		return str_compare_fn(&a->string, &b->string);
	case TYPE_JSON:
		return json_compare(a->json, b->json);
	case TYPE_INTERVAL:
		return interval_compare(&a->interval, &b->interval);
	case TYPE_VECTOR:
		return vector_compare(a->vector, b->vector);
	case TYPE_UUID:
		return uuid_compare(&a->uuid, &b->uuid);
	// TYPE_AVG
	// TYPE_REF
	// TYPE_STORE
	// TYPE_CURSOR
	// TYPE_CURSOR_STORE
	// TYPE_CURSOR_JSON
	default:
		break;
	}

	error("unsupported operation");
	return -1;
}

hot static inline int
store_compare(Store* self, Value* row_a, Value* row_b)
{
	for (int i = 0; i < self->keys; i++)
	{
		auto key_a = row_a + self->columns + i;
		auto key_b = row_b + self->columns + i;
		int rc = value_compare(key_a, key_b);
		if (rc != 0)
			return self->keys_order[i] ? rc : -rc;
	}
	return 0;
}

hot static inline bool
store_compare_keys(Store* self, Value* key_a, Value* key_b)
{
	for (int i = 0; i < self->keys; i++)
	{
		int rc = value_compare(&key_a[i], &key_b[i]);
		if (rc != 0)
			return false;
	}
	return true;
}

hot static inline bool
store_compare_keys_ptr(Store* self, Value* key_a, Value** key_b)
{
	for (int i = 0; i < self->keys; i++)
	{
		int rc = value_compare(&key_a[i], key_b[i]);
		if (rc != 0)
			return false;
	}
	return true;
}
