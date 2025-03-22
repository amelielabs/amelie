
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
#include <amelie_content.h>
#include <amelie_executor.h>
#include <amelie_func.h>
#include <amelie_vm.h>
#include <amelie_parser.h>

static inline bool
parse_endpoint_path(Str* self, Str* schema, Str* name)
{
	// /v1/db/schema/table
	if (unlikely(str_empty(self)))
		return false;

	auto pos = self->pos;
	auto end = self->end;

	// /v1/db/
	if (unlikely(! str_is_prefix(self, "/v1/db/", 7)))
		return false;
	pos += 7;

	// schema
	auto start = pos;
	while (pos < end && *pos != '/')
		pos++;
	if (unlikely(pos == end))
		return false;
	str_set(schema, start, pos - start);
	if (unlikely(str_empty(schema)))
		return false;
	pos++;

	// name
	str_set(name, pos, end - pos);
	if (unlikely(str_empty(name)))
		return false;

	return true;
}

static inline bool
parse_endpoint_column(char** pos, char* end, Str* name)
{
	// name
	// name [,]
	auto start = *pos;
	while (*pos < end && **pos != ',')
		(*pos)++;
	str_set(name, start, *pos - start);
	if (*pos != end)
		(*pos)++;
	return !str_empty(name);
}

static void
parse_endpoint_columns(Endpoint* endpoint, Str* value)
{
	endpoint->columns_has = true;
	if (str_empty(value))
		return;

	// name[, ...]
	auto pos = value->pos;
	auto end = value->end;

	auto columns = table_columns(endpoint->table);
	Ast* last = NULL;

	Str name;
	while (parse_endpoint_column(&pos, end, &name))
	{
		auto column = columns_find(columns, &name);
		if (! column)
			error("column '%.*s' does not exists", str_size(&name),
			      str_of(&name));

		if (last && column->order <= last->column->order)
			error("column list must be ordered");

		Ast* ref = ast(0);
		ref->column = column;
		if (endpoint->columns == NULL)
			endpoint->columns = ref;
		else
			last->next = ref;
		last = ref;
		endpoint->columns_count++;
	}
}

void
parse_endpoint(Endpoint* endpoint, Db* db)
{
	// POST /v1/db/schema/table <?columns=...>
	auto uri = endpoint->uri;

	// get target name
	Str schema;
	Str name;
	str_init(&schema);
	str_init(&name);
	if (unlikely(! parse_endpoint_path(&uri->path, &schema, &name)))
		error("unsupported URI path");

	// find table
	endpoint->table = table_mgr_find(&db->table_mgr, &schema, &name, true);

	// validate arguments
	list_foreach(&uri->args)
	{
		auto arg = list_at(UriArg, link);

		// columns=
		if (str_is(&arg->name, "columns", 7))
		{
			if (unlikely(endpoint->columns_has))
				error("columns argument redefined");

			parse_endpoint_columns(endpoint, &arg->value);
			continue;
		}

		error("unsupported URI argument '%.*s'", str_size(&arg->name),
		      str_of(&arg->name));
	}
}
