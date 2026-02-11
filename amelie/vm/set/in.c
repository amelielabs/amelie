
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

#include <amelie_runtime>
#include <amelie_server>
#include <amelie_db>
#include <amelie_sync>
#include <amelie_value.h>
#include <amelie_set.h>

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

		if (in[i].type == TYPE_STORE)
		{
			auto it = store_iterator(in[i].store);
			defer(store_iterator_close, it);
			Value* at;
			for (; (at = store_iterator_at(it)); store_iterator_next(it))
			{
				if (at->type == TYPE_NULL) {
					has_null = true;
					continue;
				}
				if (! value_compare(value, at))
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
