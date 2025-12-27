
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
#include <amelie_engine>
#include <amelie_storage>
#include <amelie_repl>
#include <amelie_vm>
#include <amelie_parser.h>

static void
parse_assign(Parser* self, Block* block, Ast* name)
{
	// SELECT expr INTO var
	auto stmt = stmt_allocate(self, &self->lex, block);
	stmts_add(&block->stmts, stmt);

	auto select = parse_select_expr(stmt, &name->string);
	stmt->id  = STMT_SELECT;
	stmt->ast = &select->ast;
	stmt->ret = &select->ret;
	select->ast.pos_start = name->pos_start;
	select->ast.pos_end   = name->pos_end;
	parse_select_resolve(stmt);
}

static void
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
parse_declare(Parser* self, Block* block, Ast* name, Ast* name_type)
{
	auto lex = &self->lex;

	// validate block for declare
	if (block != block->ns->blocks.list)
		lex_error(lex, name, "variable cannot be defined here");

	// type
	if (name_type->id != KNAME)
		lex_error(lex, name_type, "unrecognized data type");

	int type;
	if (str_is_case(&name_type->string, "table", 5))
	{
		type = TYPE_STORE;
	} else
	{
		lex_push(lex, name_type);
		int type_size;
		type = parse_type(lex, &type_size);
	}

	// create variable
	auto var = vars_find(&block->ns->vars, &name->string);
	if (var)
		lex_error(lex, name, "variable redefined");
	var = vars_add(&block->ns->vars, &name->string, type, false);

	// var table(column, ...)
	if (var->type == TYPE_STORE)
		parse_declare_columns(self, &var->columns);
}

void
parse_declare_or_assign(Parser* self, Block* block)
{
	// [DECLARE] var type
	// [DECLARE] var type = expr
	// var = expr
	auto lex = &self->lex;

	// [DECLARE]
	auto declare = lex_if(lex, KDECLARE);

	// name
	auto name = lex_expect(lex, KNAME);

	// := | =
	auto ast = lex_next_shadow(lex);
	if (ast->id == KASSIGN || ast->id == '=')
	{
		if (declare)
			lex_error(lex, ast, "data type expected");

		parse_assign(self, block, name);
		return;
	}

	// type
	parse_declare(self, block, name, ast);

	// [:= | =]
	if (lex_if(lex, KASSIGN) || lex_if(lex, '='))
		parse_assign(self, block, name);
}
