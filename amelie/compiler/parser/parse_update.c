
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
#include <amelie_http.h>
#include <amelie_client.h>
#include <amelie_server.h>
#include <amelie_row.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_value.h>
#include <amelie_store.h>
#include <amelie_executor.h>
#include <amelie_vm.h>
#include <amelie_parser.h>

hot Ast*
parse_update_expr(Stmt* self)
{
	// SET
	if (! stmt_if(self, KSET))
		error("UPDATE <SET> expected");

	// column = expr [, ... ]
	Ast* expr_prev = NULL;
	Ast* expr = NULL;
	for (;;)
	{
		auto op = ast(KSET);

		// name
		op->l = stmt_if(self, KNAME);
		if (! op->l)
			error("UPDATE name SET <name> expected");

		// =
		if (! stmt_if(self, '='))
			error("UPDATE name SET column <=> expected");

		// expr
		op->r = parse_expr(self, NULL);

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
parse_update_aggregated(Stmt* self, Columns* columns)
{
	auto lex_origin = self->lex;
	Lex lex;
	lex_init(&lex, keywords);
	self->lex = &lex;

	Ast* expr_prev = NULL;
	Ast* expr = NULL;
	list_foreach(&columns->list)
	{
		auto column = list_at(Column, link);
		if (str_empty(&column->constraint.as_aggregated))
			continue;

		// column = <aggregated expr>
		auto op = ast(KSET);

		// column name
		op->l = ast(KNAME);
		op->l->string = column->name;

		// parse aggregated expression
		lex_init(&lex, keywords);
		lex_start(&lex, &column->constraint.as_aggregated);
		Expr ctx =
		{
			.aggs   = NULL,
			.select = false
		};
		op->r = parse_expr(self, &ctx);

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
	auto stmt = ast_update_allocate();
	self->ast = &stmt->ast;

	// table_name, expression or join
	int level = target_list_next_level(&self->target_list);
	stmt->target = parse_from(self, level);
	stmt->table = stmt->target->from_table;
	if (stmt->table == NULL)
		error("UPDATE <table name> expected");
	if (stmt->target->next_join)
		error("UPDATE JOIN is not supported");
	if (stmt->target->from_table_index)
		if (table_primary(stmt->table) != stmt->target->from_table_index)
			error("UPDATE only primary index supported");

	// SET column = expr [, ... ]
	stmt->expr_update = parse_update_expr(self);

	// [WHERE]
	if (stmt_if(self, KWHERE))
		stmt->expr_where = parse_expr(self, NULL);

	// [RETURNING]
	if (stmt_if(self, KRETURNING))
	{
		parse_returning(&stmt->ret, self, NULL);
		parse_returning_resolve(&stmt->ret, self, stmt->target);
	}
}
