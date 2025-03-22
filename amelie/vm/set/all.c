
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

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_json.h>
#include <amelie_config.h>
#include <amelie_user.h>
#include <amelie_auth.h>
#include <amelie_http.h>
#include <amelie_client.h>
#include <amelie_server.h>
#include <amelie_row.h>
#include <amelie_heap.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_backup.h>
#include <amelie_repl.h>
#include <amelie_value.h>
#include <amelie_set.h>

hot static inline bool
value_all_array_equ(uint8_t* pos, Value* value, bool* has_null)
{
	for (json_read_array(&pos); !json_read_array_end(&pos);
	     json_skip(&pos))
	{
		Value ref;
		value_init(&ref);
		value_decode(&ref, pos, NULL);
		if (ref.type == TYPE_NULL) {
			*has_null = true;
			continue;
		}
		if (value_compare(value, &ref) != 0)
			return false;
	}
	return true;
}

hot static inline bool
value_all_array_nequ(uint8_t* pos, Value* value, bool* has_null)
{
	for (json_read_array(&pos); !json_read_array_end(&pos);
	     json_skip(&pos))
	{
		Value ref;
		value_init(&ref);
		value_decode(&ref, pos, NULL);
		if (ref.type == TYPE_NULL) {
			*has_null = true;
			continue;
		}
		if (! value_compare(value, &ref))
			return false;
	}
	return true;
}

hot static inline bool
value_all_array_lt(uint8_t* pos, Value* value, bool* has_null)
{
	for (json_read_array(&pos); !json_read_array_end(&pos);
	     json_skip(&pos))
	{
		Value ref;
		value_init(&ref);
		value_decode(&ref, pos, NULL);
		if (ref.type == TYPE_NULL) {
			*has_null = true;
			continue;
		}
		if (! (value_compare(value, &ref) < 0))
			return false;
	}
	return true;
}

hot static inline bool
value_all_array_lte(uint8_t* pos, Value* value, bool* has_null)
{
	for (json_read_array(&pos); !json_read_array_end(&pos);
	     json_skip(&pos))
	{
		Value ref;
		value_init(&ref);
		value_decode(&ref, pos, NULL);
		if (ref.type == TYPE_NULL) {
			*has_null = true;
			continue;
		}
		if (! (value_compare(value, &ref) <= 0))
			return false;
	}
	return true;
}

hot static inline bool
value_all_array_gt(uint8_t* pos, Value* value, bool* has_null)
{
	for (json_read_array(&pos); !json_read_array_end(&pos);
	     json_skip(&pos))
	{
		Value ref;
		value_init(&ref);
		value_decode(&ref, pos, NULL);
		if (ref.type == TYPE_NULL) {
			*has_null = true;
			continue;
		}
		if (! (value_compare(value, &ref) > 0))
			return false;
	}
	return true;
}

hot static inline bool
value_all_array_gte(uint8_t* pos, Value* value, bool* has_null)
{
	for (json_read_array(&pos); !json_read_array_end(&pos);
	     json_skip(&pos))
	{
		Value ref;
		value_init(&ref);
		value_decode(&ref, pos, NULL);
		if (ref.type == TYPE_NULL) {
			*has_null = true;
			continue;
		}
		if (! (value_compare(value, &ref) >= 0))
			return false;
	}
	return true;
}

hot static inline bool
value_all_equ(StoreIterator* it, Value* value, bool* has_null)
{
	Value* at;
	for (; (at = store_iterator_at(it)); store_iterator_next(it))
	{
		if (at->type == TYPE_NULL) {
			*has_null = true;
			continue;
		}
		if (value_compare(value, at) != 0)
			return false;
	}
	return true;
}

hot static inline bool
value_all_nequ(StoreIterator* it, Value* value, bool* has_null)
{
	Value* at;
	for (; (at = store_iterator_at(it)); store_iterator_next(it))
	{
		if (at->type == TYPE_NULL) {
			*has_null = true;
			continue;
		}
		if (! value_compare(value, at))
			return false;
	}
	return true;
}

hot static inline bool
value_all_lte(StoreIterator* it, Value* value, bool* has_null)
{
	Value* at;
	for (; (at = store_iterator_at(it)); store_iterator_next(it))
	{
		if (at->type == TYPE_NULL) {
			*has_null = true;
			continue;
		}
		if (! (value_compare(value, at) <= 0))
			return false;
	}
	return true;
}

hot static inline bool
value_all_lt(StoreIterator* it, Value* value, bool* has_null)
{
	Value* at;
	for (; (at = store_iterator_at(it)); store_iterator_next(it))
	{
		if (at->type == TYPE_NULL) {
			*has_null = true;
			continue;
		}
		if (! (value_compare(value, at) < 0))
			return false;
	}
	return true;
}

hot static inline bool
value_all_gte(StoreIterator* it, Value* value, bool* has_null)
{
	Value* at;
	for (; (at = store_iterator_at(it)); store_iterator_next(it))
	{
		if (at->type == TYPE_NULL) {
			*has_null = true;
			continue;
		}
		if (! (value_compare(value, at) >= 0))
			return false;
	}
	return true;
}

hot static inline bool
value_all_gt(StoreIterator* it, Value* value, bool* has_null)
{
	Value* at;
	for (; (at = store_iterator_at(it)); store_iterator_next(it))
	{
		if (at->type == TYPE_NULL) {
			*has_null = true;
			continue;
		}
		if (! (value_compare(value, at) > 0))
			return false;
	}
	return true;
}

void
value_all(Value* result, Value* a, Value* b, int op)
{
	bool match = false;
	bool has_null = false;
	if (b->type == TYPE_NULL)
	{
		has_null = true;
	} else
	if (b->type == TYPE_JSON && json_is_array(b->json))
	{
		uint8_t* pos = b->json;
		switch (op) {
		case MATCH_EQU:
			match = value_all_array_equ(pos, a, &has_null);
			break;
		case MATCH_NEQU:
			match = value_all_array_nequ(pos, a, &has_null);
			break;
		case MATCH_LT:
			match = value_all_array_lt(pos, a, &has_null);
			break;
		case MATCH_LTE:
			match = value_all_array_lte(pos, a, &has_null);
			break;
		case MATCH_GT:
			match = value_all_array_gt(pos, a, &has_null);
			break;
		case MATCH_GTE:
			match = value_all_array_gte(pos, a, &has_null);
			break;
		}
	} else
	if (b->type == TYPE_STORE)
	{
		auto it = store_iterator(b->store);
		defer(store_iterator_close, it);
		switch (op) {
		case MATCH_EQU:
			match = value_all_equ(it, a, &has_null);
			break;
		case MATCH_NEQU:
			match = value_all_nequ(it, a, &has_null);
			break;
		case MATCH_LT:
			match = value_all_lt(it, a, &has_null);
			break;
		case MATCH_LTE:
			match = value_all_lte(it, a, &has_null);
			break;
		case MATCH_GT:
			match = value_all_gt(it, a, &has_null);
			break;
		case MATCH_GTE:
			match = value_all_gte(it, a, &has_null);
			break;
		}
	} else
	{
		error("ALL: json array or subquery expected");
	}

	if (match)
	{
		if (has_null)
			value_set_null(result);
		else
			value_set_bool(result, match);
		return;
	}

	value_set_bool(result, false);
}
