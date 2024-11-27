
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

#if 0
void
value_in(Value* result, Value* store, Value* value)
{
	if (unlikely(store->type == TYPE_NULL))
	{
		value_set_null(result);
		return;
	}
	if (unlikely(store->type != TYPE_SET))
		error("IN: subquery expected");

	auto set = (Set*)store->store;
	if (unlikely(set->count_columns > 1))
		error("IN: subquery must return one column");

	for (int row = 0; row < set->count_rows ; row++)
	{
		auto at = set_column(set, row, 0);
		if (unlikely(at->type == TYPE_NULL))
		{
			value_set_null(result);
			return;
		}
		if (! value_compare(at, value))
		{
			value_set_bool(result, true);
			return;
		}
	}

	value_set_bool(result, false);
}
#endif

/*
	bool in = false;
	if (b->type == VALUE_SET)
		in = store_in(b->store, a);
	else
		in = value_is_equal(a, b);
	return value_set_bool(result, in);
	*/

/*
hot static bool
set_in(Store* store, Value* value)
{
	auto self = (Set*)store;
	int i = 0;
	for (; i < self->list_count ; i++)
	{
		auto at = set_at(self, i);
		if (value_is_equal(&at->value, value))
			return true;
	}
	return false;
}
*/
