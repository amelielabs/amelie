
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
#include <amelie_row.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_value.h>
#include <amelie_store.h>

hot static inline bool
value_all_array_equ(Value* a, Value* b, bool* has_null)
{
	auto pos = b->json;
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
		if (value_compare(a, &ref) != 0)
			return false;
	}
	return true;
}

hot static inline bool
value_all_array_nequ(Value* a, Value* b, bool* has_null)
{
	auto pos = b->json;
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
		if (! value_compare(a, &ref))
			return false;
	}
	return true;
}

hot static inline bool
value_all_array_lt(Value* a, Value* b, bool* has_null)
{
	auto pos = b->json;
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
		if (! (value_compare(a, &ref) < 0))
			return false;
	}
	return true;
}

hot static inline bool
value_all_array_lte(Value* a, Value* b, bool* has_null)
{
	auto pos = b->json;
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
		if (! (value_compare(a, &ref) <= 0))
			return false;
	}
	return true;
}

hot static inline bool
value_all_array_gt(Value* a, Value* b, bool* has_null)
{
	auto pos = b->json;
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
		if (! (value_compare(a, &ref) > 0))
			return false;
	}
	return true;
}

hot static inline bool
value_all_array_gte(Value* a, Value* b, bool* has_null)
{
	auto pos = b->json;
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
		if (! (value_compare(a, &ref) >= 0))
			return false;
	}
	return true;
}

hot static inline bool
value_all_set_equ(Value* a, Value* b, bool* has_null)
{
	auto set = (Set*)b->store;
	for (int row = 0; row < set->count_rows ; row++)
	{
		auto at = set_column(set, row, 0);
		if (at->type == TYPE_NULL) {
			*has_null = true;
			continue;
		}
		if (value_compare(a, at) != 0)
			return false;
	}
	return true;
}

hot static inline bool
value_all_set_nequ(Value* a, Value* b, bool* has_null)
{
	auto set = (Set*)b->store;
	for (int row = 0; row < set->count_rows ; row++)
	{
		auto at = set_column(set, row, 0);
		if (at->type == TYPE_NULL) {
			*has_null = true;
			continue;
		}
		if (! value_compare(a, at))
			return false;
	}
	return true;
}

hot static inline bool
value_all_set_lt(Value* a, Value* b, bool* has_null)
{
	auto set = (Set*)b->store;
	for (int row = 0; row < set->count_rows ; row++)
	{
		auto at = set_column(set, row, 0);
		if (at->type == TYPE_NULL) {
			*has_null = true;
			continue;
		}
		if (! (value_compare(a, at) < 0))
			return false;
	}
	return true;
}

hot static inline bool
value_all_set_lte(Value* a, Value* b, bool* has_null)
{
	auto set = (Set*)b->store;
	for (int row = 0; row < set->count_rows ; row++)
	{
		auto at = set_column(set, row, 0);
		if (at->type == TYPE_NULL) {
			*has_null = true;
			continue;
		}
		if (! (value_compare(a, at) <= 0))
			return false;
	}
	return true;
}

hot static inline bool
value_all_set_gt(Value* a, Value* b, bool* has_null)
{
	auto set = (Set*)b->store;
	for (int row = 0; row < set->count_rows ; row++)
	{
		auto at = set_column(set, row, 0);
		if (at->type == TYPE_NULL) {
			*has_null = true;
			continue;
		}
		if (! (value_compare(a, at) > 0))
			return false;
	}
	return true;
}

hot static inline bool
value_all_set_gte(Value* a, Value* b, bool* has_null)
{
	auto set = (Set*)b->store;
	for (int row = 0; row < set->count_rows ; row++)
	{
		auto at = set_column(set, row, 0);
		if (at->type == TYPE_NULL) {
			*has_null = true;
			continue;
		}
		if (! (value_compare(a, at) >= 0))
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
		switch (op) {
		case MATCH_EQU:
			match = value_all_array_equ(a, b, &has_null);
			break;
		case MATCH_NEQU:
			match = value_all_array_nequ(a, b, &has_null);
			break;
		case MATCH_LT:
			match = value_all_array_lt(a, b, &has_null);
			break;
		case MATCH_LTE:
			match = value_all_array_lte(a, b, &has_null);
			break;
		case MATCH_GT:
			match = value_all_array_gt(a, b, &has_null);
			break;
		case MATCH_GTE:
			match = value_all_array_gte(a, b, &has_null);
			break;
		}
	} else
	if (b->type == TYPE_SET)
	{
		auto set = (Set*)b->store;
		if (set->count_columns > 1)
			error("ALL: subquery must return one column");

		switch (op) {
		case MATCH_EQU:
			match = value_all_set_equ(a, b, &has_null);
			break;
		case MATCH_NEQU:
			match = value_all_set_nequ(a, b, &has_null);
			break;
		case MATCH_LT:
			match = value_all_set_lt(a, b, &has_null);
			break;
		case MATCH_LTE:
			match = value_all_set_lte(a, b, &has_null);
			break;
		case MATCH_GT:
			match = value_all_set_gt(a, b, &has_null);
			break;
		case MATCH_GTE:
			match = value_all_set_gte(a, b, &has_null);
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
