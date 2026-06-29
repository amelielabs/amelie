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
compare(Comparable* self, Row* a, Row* b)
{
	const int keys = self->keys_count;
	for (auto at = 0; at < keys; at++)
	{
		auto key = &((const ComparableKey*)self->keys.start)[at];
		const void* key_a = row_at(a, key->column);
		const void* key_b = row_at(b, key->column);
		int rc;
		switch (key->type) {
		case COMPARE_I64:
		{
			// int64, timestamp
			auto va = *(const int64_t*)key_a;
			auto vb = *(const int64_t*)key_b;
			rc = (va > vb) - (va < vb);
			break;
		}
		case COMPARE_I32:
		{
			// int32
			auto va = *(const int32_t*)key_a;
			auto vb = *(const int32_t*)key_b;
			rc = (va > vb) - (va < vb);
			break;
		}
		case COMPARE_UUID:
		{
			rc = uuid_compare(key_a, key_b);
			break;
		}
		case COMPARE_STR:
		{
			uint8_t* pos_a = (uint8_t*)key_a;
			uint8_t* pos_b = (uint8_t*)key_b;
			Str str_a;
			Str str_b;
			unpack_str(&pos_a, &str_a);
			unpack_str(&pos_b, &str_b);
			rc = str_compare_fn(&str_a, &str_b);
			break;
		}
		default: __builtin_unreachable();
			break;
		}
		if (rc != 0)
			return rc;
	}
	return 0;
}
