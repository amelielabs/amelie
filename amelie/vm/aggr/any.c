
//
// amelie.
//
// Real-Time SQL Database.
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
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_value.h>
#include <amelie_aggr.h>

hot static inline void
value_any_array_equ(Value* result, Value* a, Value* b)
{
	auto pos = b->data;
	data_read_array(&pos);
	while (! data_read_array_end(&pos))
	{
		Value ref;
		value_init(&ref);
		value_read(&ref, pos, NULL);
		if (value_is_equal(a, &ref))
		{
			value_set_bool(result, true);
			break;
		}
		data_skip(&pos);
	}
}

hot static inline void
value_any_array_nequ(Value* result, Value* a, Value* b)
{
	auto pos = b->data;
	data_read_array(&pos);
	while (! data_read_array_end(&pos))
	{
		Value ref;
		value_init(&ref);
		value_read(&ref, pos, NULL);
		if (! value_is_equal(a, &ref))
		{
			value_set_bool(result, true);
			break;
		}
		data_skip(&pos);
	}
}

hot static inline void
value_any_array_lt(Value* result, Value* a, Value* b)
{
	auto pos = b->data;
	data_read_array(&pos);
	while (! data_read_array_end(&pos))
	{
		Value ref;
		value_init(&ref);
		value_read(&ref, pos, NULL);
		value_lt(result, a, &ref);
		if (result->integer)
			break;
		data_skip(&pos);
	}
}

hot static inline void
value_any_array_lte(Value* result, Value* a, Value* b)
{
	auto pos = b->data;
	data_read_array(&pos);
	while (! data_read_array_end(&pos))
	{
		Value ref;
		value_init(&ref);
		value_read(&ref, pos, NULL);
		value_lte(result, a, &ref);
		if (result->integer)
			break;
		data_skip(&pos);
	}
}

hot static inline void
value_any_array_gt(Value* result, Value* a, Value* b)
{
	auto pos = b->data;
	data_read_array(&pos);
	while (! data_read_array_end(&pos))
	{
		Value ref;
		value_init(&ref);
		value_read(&ref, pos, NULL);
		value_gt(result, a, &ref);
		if (result->integer)
			break;
		data_skip(&pos);
	}
}

hot static inline void
value_any_array_gte(Value* result, Value* a, Value* b)
{
	auto pos = b->data;
	data_read_array(&pos);
	while (! data_read_array_end(&pos))
	{
		Value ref;
		value_init(&ref);
		value_read(&ref, pos, NULL);
		value_gte(result, a, &ref);
		if (result->integer)
			break;
		data_skip(&pos);
	}
}

hot static inline void
value_any_set_equ(Value* result, Value* a, Value* b)
{
	auto set = (Set*)b->obj;
	for (int i = 0; i < set->list_count ; i++)
	{
		auto at = set_at(set, i);
		if (value_is_equal(a, &at->value))
		{
			value_set_bool(result, true);
			break;
		}
	}
}

hot static inline void
value_any_set_nequ(Value* result, Value* a, Value* b)
{
	auto set = (Set*)b->obj;
	for (int i = 0; i < set->list_count ; i++)
	{
		auto at = set_at(set, i);
		if (! value_is_equal(a, &at->value))
		{
			value_set_bool(result, true);
			break;
		}
	}
}

hot static inline void
value_any_set_lt(Value* result, Value* a, Value* b)
{
	auto set = (Set*)b->obj;
	for (int i = 0; i < set->list_count ; i++)
	{
		auto at = set_at(set, i);
		value_lt(result, a, &at->value);
		if (result->integer)
			break;
	}
}

hot static inline void
value_any_set_lte(Value* result, Value* a, Value* b)
{
	auto set = (Set*)b->obj;
	for (int i = 0; i < set->list_count ; i++)
	{
		auto at = set_at(set, i);
		value_lte(result, a, &at->value);
		if (result->integer)
			break;
	}
}

hot static inline void
value_any_set_gt(Value* result, Value* a, Value* b)
{
	auto set = (Set*)b->obj;
	for (int i = 0; i < set->list_count ; i++)
	{
		auto at = set_at(set, i);
		value_gt(result, a, &at->value);
		if (result->integer)
			break;
	}
}

hot static inline void
value_any_set_gte(Value* result, Value* a, Value* b)
{
	auto set = (Set*)b->obj;
	for (int i = 0; i < set->list_count ; i++)
	{
		auto at = set_at(set, i);
		value_gte(result, a, &at->value);
		if (result->integer)
			break;
	}
}

void
value_any(Value* result, Value* a, Value* b, int op)
{
	if (b->type == VALUE_DATA)
	{
		if (! data_is_array(b->data))
			error("ANY: array or result set expected");

		value_set_bool(result, false);
		switch (op) {
		case MATCH_EQU:
			value_any_array_equ(result, a, b);
			break;
		case MATCH_NEQU:
			value_any_array_nequ(result, a, b);
			break;
		case MATCH_LT:
			value_any_array_lt(result, a, b);
			break;
		case MATCH_LTE:
			value_any_array_lte(result, a, b);
			break;
		case MATCH_GT:
			value_any_array_gt(result, a, b);
			break;
		case MATCH_GTE:
			value_any_array_gte(result, a, b);
			break;
		}
	} else
	if (b->type == VALUE_SET)
	{
		value_set_bool(result, false);
		switch (op) {
		case MATCH_EQU:
			value_any_set_equ(result, a, b);
			break;
		case MATCH_NEQU:
			value_any_set_nequ(result, a, b);
			break;
		case MATCH_LT:
			value_any_set_lt(result, a, b);
			break;
		case MATCH_LTE:
			value_any_set_lte(result, a, b);
			break;
		case MATCH_GT:
			value_any_set_gt(result, a, b);
			break;
		case MATCH_GTE:
			value_any_set_gte(result, a, b);
			break;
		}
	} else
	{
		error("ANY: array or result set expected");
	}
}
