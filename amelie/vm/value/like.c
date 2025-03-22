
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

hot void
value_like(Value* result, Value* str, Value* pattern)
{
	if (! (str->type == TYPE_STRING && pattern->type == TYPE_STRING))
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
			utf8_forward(&pos_str);
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

			auto pos_size = utf8_sizeof(*pos);
			bool match = false;
			while (pos_str < end_str)
			{
				auto pos_str_size = utf8_sizeof(*pos_str);
				match = pos_str_size == pos_size && !memcmp(pos, pos_str, pos_size);
				pos_str += pos_str_size;
				if (match)
					break;
			}
			if (! match)
				return;
			pos += pos_size;
			continue;
		}

		// \% \_
		if (*pos == '\\')
			pos++;

		// match exact single character
		if (pos_str == end_str)
			return;

		auto pos_str_size = utf8_sizeof(*pos_str);
		auto pos_size = utf8_sizeof(*pos);
		if (! (pos_str_size == pos_size && !memcmp(pos, pos_str, pos_size)))
			return;
		pos_str += pos_str_size;
		pos += pos_size;
	}

	if (pos_str != end_str)
		return;

match:
	value_set_bool(result, true);
}
