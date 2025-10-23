
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

#include <amelie_base.h>
#include <amelie_os.h>
#include <amelie_lib.h>
#include <amelie_json.h>
#include <amelie_runtime.h>
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
#include <amelie_catalog.h>
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

hot static void
parse_args(Stmt* self, Udf* udf, Set* args)
{
	// (
	stmt_expect(self, '(');

	// prepare args
	auto row = set_reserve(args);

	// value, ...
	auto columns = &udf->config->args;
	list_foreach(&columns->list)
	{
		auto column = list_at(Column, link);
		auto column_value = &row[column->order];

		// parse and set argument value
		parse_value(self, NULL, column, column_value);

		// ,
		if (stmt_if(self, ','))
		{
			if (list_is_last(&columns->list, &column->link))
				stmt_error(self, NULL, "EXECUTE arguments mismatch");
			continue;
		}
	}

	// )
	stmt_expect(self, ')');
}

void
parse_execute(Stmt* self)
{
	// EXECUTE [schema.]function_name[(expr, ...)]
	auto stmt = ast_execute_allocate();
	self->ast = &stmt->ast;
	self->ret = &stmt->ret;

	// read schema/name
	auto path = stmt_next(self);
	stmt_push(self, path);
	Str schema;
	Str name;
	if (! parse_target(self, &schema, &name))
		stmt_error(self, path, "function name expected");

	// find function
	stmt->udf = udf_mgr_find(&share()->db->catalog.udf_mgr, &schema, &name, false);
	if (! stmt->udf)
		stmt_error(self, path, "function not found");
	auto udf = stmt->udf;

	// prepare arguments
	auto parser = self->parser;
	stmt->args = set_cache_create(parser->set_cache, &parser->program->sets);
	set_prepare(stmt->args, udf->config->args.count, 0, NULL);

	// (args)
	parse_args(self, udf, stmt->args);

	// set returning column
	if (udf->config->type == TYPE_NULL)
		return;

	auto column = column_allocate();
	column_set_name(column, &name);
	column_set_type(column, udf->config->type, -1);
	columns_add(&stmt->ret.columns, column);

	// set format
	stmt->ret.format = *self->parser->local->format;
}
