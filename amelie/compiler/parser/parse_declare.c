
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
parse_declare_columns(Parser* self, Var* var)
{
	auto lex = &self->lex;

	// (
	lex_expect(lex,'(');

	for (;;)
	{
		// column name
		auto name = lex_expect(lex, KNAME);

		// ensure column is unique
		auto column = columns_find(&var->columns, &name->string);
		if (column)
			lex_error(lex, name, "column redefined");

		// type
		auto ast = lex_next_shadow(lex);
		if (ast->id != KNAME)
			lex_error(lex, ast, "unrecognized data type");

		int type_size;
		int type = type_read(&ast->string, &type_size);
		if (type == -1)
			lex_error(lex, ast, "unrecognized data type");

		// add argument to the list
		column = column_allocate();
		column_set_name(column, &name->string);
		column_set_type(column, type, type_size);
		columns_add(&var->columns, column);

		// ,
		if (lex_if(lex, ','))
			continue;

		// )
		lex_expect(lex,')');
		break;
	}
}

Var*
parse_declare(Parser* self, Vars* vars)
{
	auto lex = &self->lex;

	// name
	auto name = lex_expect(lex, KNAME);

	// type
	auto ast = lex_next_shadow(lex);
	if (ast->id != KNAME)
		lex_error(lex, ast, "unrecognized data type");

	int type_size;
	int type;
	if (str_is_case(&ast->string, "table", 5))
		type = TYPE_STORE;
	else
		type = type_read(&ast->string, &type_size);
	if (type == -1)
		lex_error(lex, ast, "unrecognized data type");

	// create variable
	auto var = vars_find(vars, &name->string);
	if (var)
		lex_error(lex, name, "variable redefined");
	var = vars_add(vars, &name->string);
	var->type = type;

	// var table(column, ...)
	if (var->type == TYPE_STORE)
		parse_declare_columns(self, var);

	return var;
}
