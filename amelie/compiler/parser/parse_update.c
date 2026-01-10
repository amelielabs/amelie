
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
#include <amelie_tier>
#include <amelie_storage>
#include <amelie_repl>
#include <amelie_vm>
#include <amelie_parser.h>

hot Ast*
parse_update_expr(Stmt* self)
{
	// SET
	stmt_expect(self, KSET);

	// column = expr [, ... ]
	Ast* expr_prev = NULL;
	Ast* expr = NULL;
	for (;;)
	{
		auto op = ast(KSET);

		// column[.path]
		op->l = stmt_next(self);
		if (op->l->id != KNAME && op->l->id != KNAME_COMPOUND)
			stmt_error(self, op->l, "column name expected");

		// =
		stmt_expect(self, '=');

		// expr
		auto def = stmt_if(self, KDEFAULT);
		if (def)
			op->r = def;
		else
			op->r = parse_expr(self, NULL);

		// rewrite update column by path
		//
		// UPDATE SET column.path = expr as
		// UPDATE SET column = public.set(column, path, expr)
		if (op->l->id == KNAME_COMPOUND)
		{
			if (def)
				stmt_error(self, def, "DEFAULT cannot be used this way");

			// exclude path from the column name
			auto name = ast(KNAME);
			str_split(&op->l->string, &name->string, '.');

			auto path = ast(KSTRING);
			path->string = op->l->string;
			str_advance(&path->string, str_size(&name->string) + 1);
			op->l->string = name->string;

			// public.set(column, path, expr)
			auto args_list = name;
			name->next = path;
			path->next = op->r;

			// args(list_head, NULL)
			auto args = ast_args_allocate();
			args->ast.l       = args_list;
			args->ast.r       = NULL;
			args->ast.integer = 3;
			args->constable   = false;

			// func(NULL, args)
			Str fn;
			str_set(&fn, "set", 3);

			auto func = ast_func_allocate();
			func->fn    = function_mgr_find(share()->function_mgr, &fn);
			func->ast.l = NULL;
			func->ast.r = &args->ast;
			assert(func->fn);

			// replace the update expression
			op->r = &func->ast;
		}

		// op(column, expr)
		if (expr == NULL)
			expr = op;
		else
			expr_prev->next = op;
		expr_prev = op;

		// ,
		if(! stmt_if(self, ','))
			break;
	}

	return expr;
}

hot Ast*
parse_update_resolved(Stmt* self, Columns* columns)
{
	auto lex_origin = self->lex;
	Lex lex;
	lex_init(&lex, keywords_alpha);
	self->lex = &lex;

	Ast* expr_prev = NULL;
	Ast* expr = NULL;
	list_foreach(&columns->list)
	{
		auto column = list_at(Column, link);
		if (str_empty(&column->constraints.as_resolved))
			continue;

		// column = <resolved expr>
		auto op = ast(KSET);

		// column name
		op->l = ast(KNAME);
		op->l->string = column->name;

		// parse resolved expression
		lex_init(&lex, keywords_alpha);
		lex_start(&lex, &column->constraints.as_resolved);
		op->r = parse_expr(self, NULL);

		// op(column, expr)
		if (expr == NULL)
			expr = op;
		else
			expr_prev->next = op;
		expr_prev = op;
	}

	self->lex = lex_origin;
	return expr;
}

hot void
parse_update(Stmt* self)
{
	// UPDATE name SET column = expr [, ... ]
	// [WHERE expr]
	// [RETURNING expr]
	auto stmt = ast_update_allocate(self->block);
	self->ast = &stmt->ast;
	self->ret = &stmt->ret;

	// table
	parse_from(self, &stmt->from, LOCK_SHARED_RW, false);
	if (from_empty(&stmt->from) || from_is_join(&stmt->from))
		stmt_error(self, NULL, "table name expected");
	auto target = from_first(&stmt->from);
	if (! target_is_table(target))
		stmt_error(self, NULL, "table name expected");
	stmt->table = target->from_table;

	// ensure primary index is used
	if (target->from_index)
		if (table_primary(stmt->table) != target->from_index)
			stmt_error(self, NULL, "UPDATE supports only primary index");

	// SET column = expr [, ... ]
	stmt->expr_update = parse_update_expr(self);

	// [WHERE]
	if (stmt_if(self, KWHERE))
	{
		Expr ctx;
		expr_init(&ctx);
		ctx.select = true;
		ctx.names  = &stmt->from.list_names;
		ctx.from   = &stmt->from;
		stmt->expr_where = parse_expr(self, &ctx);
	}

	// [RETURNING]
	if (stmt_if(self, KRETURNING))
	{
		parse_returning(&stmt->ret, self, NULL);
		parse_returning_resolve(&stmt->ret, self, &stmt->from);
	}
}
