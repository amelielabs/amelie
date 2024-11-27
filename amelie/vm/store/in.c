
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

void
value_in(Value* result, Value* value, Value* in, int count)
{
	if (unlikely(value->type == TYPE_NULL))
	{
		value_set_null(result);
		return;
	}

	auto has_null = false;
	for (int i = 0; i < count; i++)
	{
		if (in[i].type == TYPE_NULL)
		{
			has_null = true;
			continue;
		}

		if (in[i].type == TYPE_SET)
		{
			auto set = (Set*)in[i].store;
			if (set->count_columns > 1)
				error("IN: subquery must return one column");
			for (int row = 0; row < set->count_rows ; row++)
			{
				auto at = set_column(set, row, 0);
				if (at->type == TYPE_NULL)
				{
					has_null = true;
					continue;
				}
				if (! value_compare(at, value))
				{
					value_set_bool(result, true);
					return;
				}
			}
			continue;
		}

		if (! value_compare(&in[i], value))
		{
			value_set_bool(result, true);
			return;
		}
	}

	if (has_null)
		value_set_null(result);
	else
		value_set_bool(result, false);
}
