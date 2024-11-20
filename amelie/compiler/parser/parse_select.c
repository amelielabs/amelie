
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
#include <amelie_value.h>
#include <amelie_store.h>
#include <amelie_user.h>
#include <amelie_auth.h>
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
#include <amelie_executor.h>
#include <amelie_vm.h>
#include <amelie_parser.h>

static inline void
parse_select_group_by(Stmt* self, AstSelect* select)
{
	// GROUP BY expr, ...
	Expr ctx;
	expr_init(&ctx);
	auto list = &select->expr_group_by;
	for (;;)
	{
		auto expr = parse_expr(self, &ctx);
		auto group = ast_group_allocate(list->count, expr);
		ast_list_add(list, &group->ast);

		// ,
		if (stmt_if(self, ','))
			continue;

		break;
	}
}

static inline void
parse_select_order_by(Stmt* self, AstSelect* select)
{
	// ORDER BY expr, ...
	Expr ctx;
	expr_init(&ctx);
	auto list = &select->expr_order_by;
	for (;;)
	{
		// expr
		auto expr = parse_expr(self, &ctx);

		// [ASC|DESC]
		bool asc = true;
		if (stmt_if(self, KASC))
			asc = true;
		else
		if (stmt_if(self, KDESC))
			asc = false;

		auto order = ast_order_allocate(list->count, expr, asc);
		ast_list_add(list, &order->ast);

		// ,
		if (stmt_if(self, ','))
			continue;

		break;
	}
}

static inline void
parse_select_distinct(Stmt* self, AstSelect* select)
{
	select->distinct = true;

	// [ON]
	if (! stmt_if(self, KON))
		return;

	// (
	if (! stmt_if(self, '('))
		error("DISTINCT <(> expected");

	select->distinct_on = true;

	// (expr, ...)
	Expr ctx;
	expr_init(&ctx);
	for (;;)
	{
		// expr
		auto expr = parse_expr(self, &ctx);

		// set distinct expr as order by key
		auto order = ast_order_allocate(select->expr_order_by.count, expr, true);
		ast_list_add(&select->expr_order_by, &order->ast);

		// ,
		if (stmt_if(self, ','))
			continue;

		// )
		if (! stmt_if(self, ')'))
			error("DISTINCT (<)> expected");
		break;
	}
}

hot AstSelect*
parse_select(Stmt* self)
{
	// SELECT [DISTINCT] expr, ...
	// [FROM name, [...]]
	// [GROUP BY]
	// [WHERE expr]
	// [ORDER BY]
	// [LIMIT expr] [OFFSET expr]
	int level = target_list_next_level(&self->target_list);
	auto select = ast_select_allocate();
	ast_list_add(&self->select_list, &select->ast);

	// [DISTINCT]
	if (stmt_if(self, KDISTINCT))
		parse_select_distinct(self, select);

	// * | expr [AS] [name], ...
	Expr ctx;
	expr_init(&ctx);
	ctx.select = true;
	ctx.aggs = &select->expr_aggs;
	parse_returning(&select->ret, self, &ctx);

	// [FROM]
	if (stmt_if(self, KFROM))
		select->target = parse_from(self, level);

	// create select expressions and resolve *
	parse_returning_resolve(&select->ret, self, select->target);

	// use select expr as distinct expression, if not set
	if (select->distinct && !select->distinct_on)
	{
		// set distinct expressions as order by keys
		for (auto as = select->ret.list; as; as = as->next)
		{
			auto order = ast_order_allocate(select->expr_order_by.count, as->l, true);
			ast_list_add(&select->expr_order_by, &order->ast);
		}
	}

	// [WHERE]
	if (stmt_if(self, KWHERE))
		select->expr_where = parse_expr(self, NULL);

	// use as WHERE expr or combine JOIN ON (expr) together with
	// WHERE expr per target
	if (select->target)
		select->expr_where =
			parse_from_join_on_and_where(select->target, select->expr_where);

	// [GROUP BY]
	if (stmt_if(self, KGROUP))
	{
		if (! stmt_if(self, KBY))
			error("GROUP <BY> expected");

		parse_select_group_by(self, select);

		// [HAVING]
		if (stmt_if(self, KHAVING))
			select->expr_having = parse_expr(self, &ctx);
	}

	// [ORDER BY]
	if (stmt_if(self, KORDER))
	{
		if (select->distinct)
			error("ORDER BY and DISTINCT cannot be combined");

		if (! stmt_if(self, KBY))
			error("ORDER <BY> expected");

		parse_select_order_by(self, select);
	}

	// [LIMIT]
	if (stmt_if(self, KLIMIT))
		select->expr_limit = parse_expr(self, NULL);

	// [OFFSET]
	if (stmt_if(self, KOFFSET))
		select->expr_offset = parse_expr(self, NULL);

	// add group by target
	if (select->expr_group_by.count > 0 || select->expr_aggs.count > 0)
	{
		if (select->target == NULL)
			error("no targets to use with GROUP BY or aggregates");

		// add at least one group by key
		auto list = &select->expr_group_by;
		if (! list->count)
		{
			auto expr = ast(KINT);
			expr->integer = 0;
			auto group = ast_group_allocate(list->count, expr);
			ast_list_add(list, &group->ast);
			select->expr_group_by_has = false;
		} else {
			select->expr_group_by_has = true;
		}

		// create group by target to scan agg set
		auto target = target_allocate();
		target->type = TARGET_SELECT;
		target->from_columns = &select->columns_group;
		target_list_add(&self->target_list, target, level, 0);
		select->target_group = target;
		// [aggs, group_by_keys]

		// group by target columns will be set during compile
	}

	return select;
}

hot AstSelect*
parse_select_expr(Stmt* self)
{
	// SELECT expr, ...
	auto select = ast_select_allocate();
	ast_list_add(&self->select_list, &select->ast);
	Expr ctx;
	expr_init(&ctx);
	parse_returning(&select->ret, self, &ctx);
	parse_returning_resolve(&select->ret, self, NULL);
	return select;
}
