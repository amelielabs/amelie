
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

void
parse_declare_columns(Parser* self, Columns* columns)
{
	auto lex = &self->lex;

	// (
	lex_expect(lex,'(');

	for (;;)
	{
		// column name
		auto name = lex_expect(lex, KNAME);

		// ensure column is unique
		auto column = columns_find(columns, &name->string);
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
		columns_add(columns, column);

		// ,
		if (lex_if(lex, ','))
			continue;

		// )
		lex_expect(lex,')');
		break;
	}
}

static void
parse_assign_stmt(Parser* self, Block* block, Str* name)
{
	// SELECT expr INTO var
	auto stmt = stmt_allocate(self, &self->lex, block);
	stmts_add(&block->stmts, stmt);

	auto select = parse_select_expr(stmt, name);
	stmt->id  = STMT_SELECT;
	stmt->ast = &select->ast;
	stmt->ret = &select->ret;
	parse_select_resolve(stmt);
}

void
parse_declare(Parser* self, Block* block)
{
	// name
	auto lex = &self->lex;
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
	auto var = vars_find(&block->ns->vars, &name->string);
	if (var)
		lex_error(lex, name, "variable redefined");
	var = vars_add(&block->ns->vars, &name->string, type, false);

	// var table(column, ...)
	if (var->type == TYPE_STORE)
		parse_declare_columns(self, &var->columns);

	// := expr
	if (lex_if(lex, KASSIGN))
		parse_assign_stmt(self, block, var->name);
}

void
parse_assign(Parser* self, Block* block)
{
	// var := expr

	// name
	auto lex = &self->lex;
	auto name = lex_expect(lex, KNAME);

	// :=
	lex_expect(lex, KASSIGN);

	// expr
	parse_assign_stmt(self, block, &name->string);
}
