
//
// sonata.
//
// Real-Time SQL Database.
//

#include <sonata_runtime.h>
#include <sonata_io.h>
#include <sonata_lib.h>
#include <sonata_data.h>
#include <sonata_config.h>
#include <sonata_row.h>
#include <sonata_transaction.h>
#include <sonata_index.h>
#include <sonata_partition.h>
#include <sonata_wal.h>
#include <sonata_db.h>
#include <sonata_value.h>
#include <sonata_aggr.h>

hot static inline void
value_all_array_equ(Value* result, Value* a, Value* b)
{
	auto pos = b->data;
	data_read_array(&pos);
	while (! data_read_array_end(&pos))
	{
		Value ref;
		value_init(&ref);
		value_read(&ref, pos, NULL);
		if (! value_is_equal(a, &ref))
			return;
		data_skip(&pos);
	}
	value_set_bool(result, true);
}

hot static inline void
value_all_array_nequ(Value* result, Value* a, Value* b)
{
	auto pos = b->data;
	data_read_array(&pos);
	while (! data_read_array_end(&pos))
	{
		Value ref;
		value_init(&ref);
		value_read(&ref, pos, NULL);
		if (value_is_equal(a, &ref))
			return;
		data_skip(&pos);
	}
	value_set_bool(result, true);
}

hot static inline void
value_all_array_lt(Value* result, Value* a, Value* b)
{
	auto pos = b->data;
	data_read_array(&pos);
	while (! data_read_array_end(&pos))
	{
		Value ref;
		value_init(&ref);
		value_read(&ref, pos, NULL);
		value_lt(result, a, &ref);
		if (! result->integer)
			return;
		data_skip(&pos);
	}
}

hot static inline void
value_all_array_lte(Value* result, Value* a, Value* b)
{
	auto pos = b->data;
	data_read_array(&pos);
	while (! data_read_array_end(&pos))
	{
		Value ref;
		value_init(&ref);
		value_read(&ref, pos, NULL);
		value_lte(result, a, &ref);
		if (! result->integer)
			return;
		data_skip(&pos);
	}
}

hot static inline void
value_all_array_gt(Value* result, Value* a, Value* b)
{
	auto pos = b->data;
	data_read_array(&pos);
	while (! data_read_array_end(&pos))
	{
		Value ref;
		value_init(&ref);
		value_read(&ref, pos, NULL);
		value_gt(result, a, &ref);
		if (! result->integer)
			return;
		data_skip(&pos);
	}
}

hot static inline void
value_all_array_gte(Value* result, Value* a, Value* b)
{
	auto pos = b->data;
	data_read_array(&pos);
	while (! data_read_array_end(&pos))
	{
		Value ref;
		value_init(&ref);
		value_read(&ref, pos, NULL);
		value_gte(result, a, &ref);
		if (! result->integer)
			return;
		data_skip(&pos);
	}
}

hot static inline void
value_all_set_equ(Value* result, Value* a, Value* b)
{
	auto set = (Set*)b->obj;
	for (int i = 0; i < set->list_count ; i++)
	{
		auto at = set_at(set, i);
		if (! value_is_equal(a, &at->value))
			return;
	}
	value_set_bool(result, true);
}

hot static inline void
value_all_set_nequ(Value* result, Value* a, Value* b)
{
	auto set = (Set*)b->obj;
	for (int i = 0; i < set->list_count ; i++)
	{
		auto at = set_at(set, i);
		if (value_is_equal(a, &at->value))
			return;
	}
	value_set_bool(result, true);
}

hot static inline void
value_all_set_lt(Value* result, Value* a, Value* b)
{
	auto set = (Set*)b->obj;
	for (int i = 0; i < set->list_count ; i++)
	{
		auto at = set_at(set, i);
		value_lt(result, a, &at->value);
		if (! result->integer)
			return;
	}
}

hot static inline void
value_all_set_lte(Value* result, Value* a, Value* b)
{
	auto set = (Set*)b->obj;
	for (int i = 0; i < set->list_count ; i++)
	{
		auto at = set_at(set, i);
		value_lte(result, a, &at->value);
		if (! result->integer)
			return;
	}
}

hot static inline void
value_all_set_gt(Value* result, Value* a, Value* b)
{
	auto set = (Set*)b->obj;
	for (int i = 0; i < set->list_count ; i++)
	{
		auto at = set_at(set, i);
		value_gt(result, a, &at->value);
		if (! result->integer)
			return;
	}
}

hot static inline void
value_all_set_gte(Value* result, Value* a, Value* b)
{
	auto set = (Set*)b->obj;
	for (int i = 0; i < set->list_count ; i++)
	{
		auto at = set_at(set, i);
		value_gte(result, a, &at->value);
		if (! result->integer)
			return;
	}
}

void
value_all(Value* result, Value* a, Value* b, int op)
{
	if (b->type == VALUE_DATA)
	{
		if (! data_is_array(b->data))
			error("ALL: array or result set expected");

		value_set_bool(result, false);
		switch (op) {
		case MATCH_EQU:
			value_all_array_equ(result, a, b);
			break;
		case MATCH_NEQU:
			value_all_array_nequ(result, a, b);
			break;
		case MATCH_LT:
			value_all_array_lt(result, a, b);
			break;
		case MATCH_LTE:
			value_all_array_lte(result, a, b);
			break;
		case MATCH_GT:
			value_all_array_gt(result, a, b);
			break;
		case MATCH_GTE:
			value_all_array_gte(result, a, b);
			break;
		}
	} else
	if (b->type == VALUE_SET)
	{
		value_set_bool(result, false);
		switch (op) {
		case MATCH_EQU:
			value_all_set_equ(result, a, b);
			break;
		case MATCH_NEQU:
			value_all_set_nequ(result, a, b);
			break;
		case MATCH_LT:
			value_all_set_lt(result, a, b);
			break;
		case MATCH_LTE:
			value_all_set_lte(result, a, b);
			break;
		case MATCH_GT:
			value_all_set_gt(result, a, b);
			break;
		case MATCH_GTE:
			value_all_set_gte(result, a, b);
			break;
		}
	} else
	{
		error("ALL: array or result set expected");
	}
}
