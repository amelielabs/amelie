
//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
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

hot void
value_like(Value* result, Value* str, Value* pattern)
{
	if (! (str->type == VALUE_STRING && pattern->type == VALUE_STRING))
		error("LIKE: arguments expected to be strings");

	value_set_bool(result, false);

	auto pos_str = str->string.pos;
	auto end_str = str->string.end;
	auto pos = pattern->string.pos;
	auto end = pattern->string.end;
	while (pos < end)
	{
		// match any single character
		if (*pos == '_')
		{
			if (pos_str == end_str)
				return;
			pos_str++;
			pos++;
			continue;
		}

		// match zero, one or multiple characters
		if (*pos == '%')
		{
			// combine % and _ together
			while (pos < end && (*pos == '%' || *pos == '_'))
				pos++;

			// % at the end of the pattern
			if (pos == end)
				goto match;

			// match next character after %
			if (*pos == '\\')
				pos++;
			if (pos == end)
				return;

			bool match = false;
			for (; pos_str < end_str; pos_str++)
			{
				if (*pos_str == *pos)
				{
					match = true;
					break;
				}
			}
			if (! match)
				return;
			pos_str++;
			pos++;
			continue;
		}

		// \% \_
		if (*pos == '\\')
			pos++;

		// match exact single character
		if (pos_str == end_str)
			return;
		if (*pos_str != *pos)
			return;
		pos_str++;
		pos++;
	}

	if (pos_str != end_str)
		return;

match:
	value_set_bool(result, true);
}
