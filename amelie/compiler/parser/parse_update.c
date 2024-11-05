
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
#include <amelie_data.h>
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

	// path = expr [, ... ]
	Ast* expr_prev = NULL;
	Ast* expr = NULL;
	for (;;)
	{
		auto op = ast(KSET);

		// name or path
		op->l = stmt_next(self);
		switch (op->l->id) {
		case KNAME:
		case KNAME_COMPOUND:
			break;
		default:
			error("UPDATE name SET <path> expected");
			break;
		}

		// =
		if (! stmt_if(self, '='))
			error("UPDATE name SET path <=> expected");

		// expr
		op->r = parse_expr(self, NULL);

		// op(path, expr)
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
		if (column->constraint.not_aggregated)
			continue;

		// column = <aggregated expr>
		auto op = ast(KSET);

		// column name
		op->l = ast(KNAME);
		op->l->string = column->name;

		// choose aggregated expr
		Str* as_expr;
		Str  as_expr_agg;
		if (! str_empty(&column->constraint.as_aggregated))
		{
			as_expr = &column->constraint.as_aggregated;
		} else
		{
			// automatically generate expression for aggregated types
			assert(column->type == TYPE_AGG);

			// agg_func(column, @[column_order])
			auto ptr = palloc(64);
			auto rc  = snprintf(ptr, 64, "agg_%s(%.*s, @[%d])",
			                    agg_nameof(column->constraint.aggregate),
			                    str_size(&column->name),
			                    str_of(&column->name),
			                    column->order);
			if (unlikely(rc <= 0 || rc >= 64))
				error("aggregated expression is too large");
			str_set(&as_expr_agg, ptr, rc);
			as_expr = &as_expr_agg;
		}

		// parse aggregated expression
		lex_init(&lex, keywords);
		lex_start(&lex, as_expr);
		Expr ctx =
		{
			.aggs   = NULL,
			.select = false
		};
		op->r = parse_expr(self, &ctx);

		// op(path, expr)
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
	// UPDATE name SET path = expr [, ... ]
	// [WHERE expr]
	// [RETURNING expr [INTO cte]]
	auto stmt = ast_update_allocate();
	self->ast = &stmt->ast;

	// table_name, expression or join
	int level = target_list_next_level(&self->target_list);
	stmt->target = parse_from(self, level);
	stmt->table = stmt->target->table;
	if (stmt->table == NULL)
		error("UPDATE <table name> expected");
	if (stmt->target->next_join)
		error("UPDATE JOIN is not supported");
	if (stmt->target->index)
		if (table_primary(stmt->table) != stmt->target->index)
			error("UPDATE only primary index supported");

	// SET path = expr [, ... ]
	stmt->expr_update = parse_update_expr(self);

	// [WHERE]
	if (stmt_if(self, KWHERE))
		stmt->expr_where = parse_expr(self, NULL);

	// [RETURNING]
	if (stmt_if(self, KRETURNING))
	{
		stmt->returning = parse_expr(self, NULL);

		// [INTO cte_name]
		if (stmt_if(self, KINTO))
			parse_cte(self, false, false);
	}
}
