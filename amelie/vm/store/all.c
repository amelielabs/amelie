
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
#include <amelie_data.h>
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
value_all_array_equ(Value* a, Value* b)
{
	auto pos = b->data;
	data_read_array(&pos);
	while (! data_read_array_end(&pos))
	{
		Value ref;
		value_init(&ref);
		value_decode(&ref, pos, NULL);
		if (value_compare(a, &ref) != 0)
			return false;
		data_skip(&pos);
	}
	return true;
}

hot static inline bool
value_all_array_nequ(Value* a, Value* b)
{
	auto pos = b->data;
	data_read_array(&pos);
	while (! data_read_array_end(&pos))
	{
		Value ref;
		value_init(&ref);
		value_decode(&ref, pos, NULL);
		if (! value_compare(a, &ref))
			return false;
		data_skip(&pos);
	}
	return true;
}

hot static inline bool
value_all_array_lt(Value* a, Value* b)
{
	auto pos = b->data;
	data_read_array(&pos);
	while (! data_read_array_end(&pos))
	{
		Value ref;
		value_init(&ref);
		value_decode(&ref, pos, NULL);
		if (! (value_compare(a, &ref) < 0))
			return false;
		data_skip(&pos);
	}
	return true;
}

hot static inline bool
value_all_array_lte(Value* a, Value* b)
{
	auto pos = b->data;
	data_read_array(&pos);
	while (! data_read_array_end(&pos))
	{
		Value ref;
		value_init(&ref);
		value_decode(&ref, pos, NULL);
		if (! (value_compare(a, &ref) <= 0))
			return false;
		data_skip(&pos);
	}
	return true;
}

hot static inline bool
value_all_array_gt(Value* a, Value* b)
{
	auto pos = b->data;
	data_read_array(&pos);
	while (! data_read_array_end(&pos))
	{
		Value ref;
		value_init(&ref);
		value_decode(&ref, pos, NULL);
		if (! (value_compare(a, &ref) > 0))
			return false;
		data_skip(&pos);
	}
	return true;
}

hot static inline bool
value_all_array_gte(Value* a, Value* b)
{
	auto pos = b->data;
	data_read_array(&pos);
	while (! data_read_array_end(&pos))
	{
		Value ref;
		value_init(&ref);
		value_decode(&ref, pos, NULL);
		if (! (value_compare(a, &ref) >= 0))
			return false;
		data_skip(&pos);
	}
	return true;
}

hot static inline bool
value_all_set_equ(Value* a, Value* b)
{
	auto set = (Set*)b->store;
	for (int row = 0; row < set->count_rows ; row++)
	{
		auto at = set_column(set, row, 0);
		if (value_compare(a, at) != 0)
			return false;
	}
	return true;
}

hot static inline bool
value_all_set_nequ(Value* a, Value* b)
{
	auto set = (Set*)b->store;
	for (int row = 0; row < set->count_rows ; row++)
	{
		auto at = set_column(set, row, 0);
		if (! value_compare(a, at))
			return false;
	}
	return true;
}

hot static inline bool
value_all_set_lt(Value* a, Value* b)
{
	auto set = (Set*)b->store;
	for (int row = 0; row < set->count_rows ; row++)
	{
		auto at = set_column(set, row, 0);
		if (! (value_compare(a, at) < 0))
			return false;
	}
	return true;
}

hot static inline bool
value_all_set_lte(Value* a, Value* b)
{
	auto set = (Set*)b->store;
	for (int row = 0; row < set->count_rows ; row++)
	{
		auto at = set_column(set, row, 0);
		if (! (value_compare(a, at) <= 0))
			return false;
	}
	return true;
}

hot static inline bool
value_all_set_gt(Value* a, Value* b)
{
	auto set = (Set*)b->store;
	for (int row = 0; row < set->count_rows ; row++)
	{
		auto at = set_column(set, row, 0);
		if (! (value_compare(a, at) > 0))
			return false;
	}
	return true;
}

hot static inline bool
value_all_set_gte(Value* a, Value* b)
{
	auto set = (Set*)b->store;
	for (int row = 0; row < set->count_rows ; row++)
	{
		auto at = set_column(set, row, 0);
		if (! (value_compare(a, at) >= 0))
			return false;
	}
	return true;
}

void
value_all(Value* result, Value* a, Value* b, int op)
{
	if (b->type == VALUE_JSON && data_is_array(b->data))
	{
		bool match;
		switch (op) {
		case MATCH_EQU:
			match = value_all_array_equ(a, b);
			break;
		case MATCH_NEQU:
			match = value_all_array_nequ(a, b);
			break;
		case MATCH_LT:
			match = value_all_array_lt(a, b);
			break;
		case MATCH_LTE:
			match = value_all_array_lte(a, b);
			break;
		case MATCH_GT:
			match = value_all_array_gt(a, b);
			break;
		case MATCH_GTE:
			match = value_all_array_gte(a, b);
			break;
		}
		value_set_bool(result, match);
	} else
	if (b->type == VALUE_SET)
	{
		bool match;
		switch (op) {
		case MATCH_EQU:
			match = value_all_set_equ(a, b);
			break;
		case MATCH_NEQU:
			match = value_all_set_nequ(a, b);
			break;
		case MATCH_LT:
			match = value_all_set_lt(a, b);
			break;
		case MATCH_LTE:
			match = value_all_set_lte(a, b);
			break;
		case MATCH_GT:
			match = value_all_set_gt(a, b);
			break;
		case MATCH_GTE:
			match = value_all_set_gte(a, b);
			break;
		}
		value_set_bool(result, match);
	} else
	{
		error("ALL: json array or result set expected");
	}
}
