
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
#include <amelie_vm>
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
	// EXECUTE [user.]function_name[(expr, ...)]
	auto stmt = ast_execute_allocate();
	self->ast = &stmt->ast;
	self->ret = &stmt->ret;

	// read name
	Str user;
	Str name;
	auto path = parse_target(self, &user, &name);

	// find function
	stmt->udf = catalog_find_udf(&share()->db->catalog, &user, &name, false);
	if (! stmt->udf)
		stmt_error(self, path, "function '{str}.{str}': not found", &user, &name);
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
	column_set_type(column, udf->config->type, -1, 0);
	columns_add(&stmt->ret.columns, column);
}
