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

hot static inline int
set_compare(Set* self, Value* row_a, Value* row_b)
{
	for (int i = 0; i < self->count_keys; i++)
	{
		auto key_a = row_a + self->count_columns + i;
		auto key_b = row_b + self->count_columns + i;
		int rc = value_compare(key_a, key_b);
		if (rc != 0)
			return self->ordered[i] ? rc : -rc;
	}
	return 0;
}

hot static inline bool
set_compare_keys_n(Value* key_a, Value* key_b, int count)
{
	for (int i = 0; i < count; i++)
	{
		int rc = value_compare(&key_a[i], &key_b[i]);
		if (rc != 0)
			return false;
	}
	return true;
}

hot static inline bool
set_compare_keys(Set* self, Value* key_a, Value* key_b)
{
	return set_compare_keys_n(key_a, key_b, self->count_keys);
}
