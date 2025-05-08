
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
	ctx.aggs = &select->expr_aggs;

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
	stmt_expect(self, '(');
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
		stmt_expect(self, ')');
		break;
	}
}

hot AstSelect*
parse_select(Stmt* self, Targets* outer, bool subquery)
{
	// SELECT [DISTINCT] expr, ...
	// [INTO name]
	// [FORMAT name]
	// [FROM name, [...]]
	// [GROUP BY]
	// [WHERE expr]
	// [ORDER BY]
	// [LIMIT expr] [OFFSET expr]
	auto select = ast_select_allocate(outer, self->scope);
	ast_list_add(&self->select_list, &select->ast);

	// [DISTINCT]
	if (stmt_if(self, KDISTINCT))
		parse_select_distinct(self, select);

	// * | expr [AS] [name], ...
	Expr ctx;
	expr_init(&ctx);
	ctx.select = true;
	ctx.subquery = subquery;
	ctx.aggs = &select->expr_aggs;
	ctx.targets = &select->targets;
	parse_returning(&select->ret, self, &ctx);

	// [FROM]
	if (stmt_if(self, KFROM))
	{
		parse_from(self, &select->targets,
		           subquery ? ACCESS_RO_EXCLUSIVE : ACCESS_RO,
		           subquery);
	}

	// [WHERE]
	if (stmt_if(self, KWHERE))
	{
		Expr ctx_where;
		expr_init(&ctx_where);
		ctx_where.select = true;
		ctx_where.subquery = subquery;
		ctx_where.targets = &select->targets;
		select->expr_where = parse_expr(self, &ctx_where);
	}

	// use as WHERE expr or combine JOIN ON (expr) together with
	// WHERE expr per target
	if (targets_is_join(&select->targets))
		select->expr_where =
			parse_from_join_on_and_where(&select->targets, select->expr_where);

	// [GROUP BY]
	if (stmt_if(self, KGROUP))
	{
		stmt_expect(self, KBY);
		parse_select_group_by(self, select);

		// [HAVING]
		if (stmt_if(self, KHAVING))
			select->expr_having = parse_expr(self, &ctx);
	}

	// [ORDER BY]
	if (stmt_if(self, KORDER))
	{
		stmt_expect(self, KBY);
		parse_select_order_by(self, select);

		if (select->distinct)
			stmt_error(self, NULL, "ORDER BY and DISTINCT cannot be combined");
	}

	// [LIMIT]
	if (stmt_if(self, KLIMIT))
	{
		select->expr_limit = parse_expr(self, NULL);
		if (select->expr_limit->id != KINT)
			stmt_error(self, select->expr_limit, "integer type expected");
		if (select->expr_limit->integer < 0)
			stmt_error(self, select->expr_limit, "positive integer value expected");
	}

	// [OFFSET]
	if (stmt_if(self, KOFFSET))
	{
		select->expr_offset = parse_expr(self, NULL);
		if (select->expr_offset->id != KINT)
			stmt_error(self, select->expr_offset, "integer type expected");
		if (select->expr_offset->integer < 0)
			stmt_error(self, select->expr_offset, "positive integer value expected");
	}

	// [FORMAT type]
	if (stmt_if(self, KFORMAT))
	{
		auto type = stmt_expect(self, KSTRING);
		select->ret.format = type->string;
	}

	// add group by target
	if (select->expr_group_by.count > 0 || select->expr_aggs.count > 0)
	{
		if (targets_empty(&select->targets))
			stmt_error(self, NULL, "no targets to use with GROUP BY or aggregation");

		// add at least one group by key
		auto list = &select->expr_group_by;
		if (! list->count)
		{
			auto expr = ast(KTRUE);
			auto group = ast_group_allocate(list->count, expr);
			ast_list_add(list, &group->ast);
			select->expr_group_by_has = false;
		} else {
			select->expr_group_by_has = true;
		}

		// create group by target to scan agg set
		auto target = target_allocate(&self->order_targets);
		target->type = TARGET_GROUP_BY;
		target->ast  = targets_outer(&select->targets)->ast;
		target->name = targets_outer(&select->targets)->name;
		target->from_group_by = &select->expr_group_by;
		target->from_columns  = &select->targets_group_columns;
		targets_add(&select->targets_group, target);

		// group by target columns will be created
		// during resolve
	}

	// set pushdown strategy for the root query
	if (! subquery)
	{
		auto outer = select->targets.list;
		if (outer && target_is_table(select->targets.list))
			select->pushdown = outer;
	}

	return select;
}

AstSelect*
parse_select_expr(Stmt* self)
{
	// SELECT expr
	auto select = ast_select_allocate(NULL, self->scope);
	ast_list_add(&self->select_list, &select->ast);
	Expr ctx;
	expr_init(&ctx);
	parse_returning(&select->ret, self, &ctx);
	return select;
}

static void
parse_select_resolve_group_by(AstSelect* select)
{
	// create columns for the group by target, actual column
	// types will be set during emit
	//
	// [aggs, group_by_keys]
	//

	// add columns for aggs
	auto node = select->expr_aggs.list;
	for (; node; node = node->next)
	{
		auto agg = ast_agg_of(node->ast);
		auto column = column_allocate();
		auto name_sz = palloc(8);
		snprintf(name_sz, 8, "_agg%d", agg->order + 1);
		Str name;
		str_set_cstr(&name, name_sz);
		column_set_name(column, &name);
		columns_add(&select->targets_group_columns, column);
		agg->column = column;
	}

	// add columns for keys
	node = select->expr_group_by.list;
	for (; node; node = node->next)
	{
		auto group = ast_group_of(node->ast);
		auto column = column_allocate();
		auto name_sz = palloc(8);
		snprintf(name_sz, 8, "_key%d", group->order + 1);
		Str name;
		str_set_cstr(&name, name_sz);
		column_set_name(column, &name);
		columns_add(&select->targets_group_columns, column);
		group->column = column;
	}
}

static void
parse_select_resolve_group_by_alias(Stmt* self, AstSelect* select)
{
	auto node = select->expr_group_by.list;
	for (; node; node = node->next)
	{
		auto group = ast_group_of(node->ast);

		// find column select returning column order (order starts from 1)
		auto order = -1;
		if (group->expr->id == KINT)
		{
			// SELECT expr, ... GROUP BY 1
			order = group->expr->integer;
		} else
		if (group->expr->id == KNAME)
		{
			// SELECT expr as, ... GROUP BY alias
			auto name = &group->expr->string;
			bool conflict = false;
			auto column = columns_find_noconflict(&select->ret.columns, name, &conflict);
			if (column)
				order = column->order + 1;
		}
		if (order == -1)
			continue;

		// get the select returning column
		auto as = returning_find(&select->ret, order);
		if (! as)
			stmt_error(self, group->expr, "column %d is not in the SELECT expr list", order);

		// ensure expression does not involve aggregates
		for (auto node = select->expr_aggs.list; node; node = node->next)
			if (ast_agg_of(node->ast)->as == as)
				stmt_error(self, group->expr, "aggregate functions cannot be used in GROUP BY expressions");

		// use the returning expression as a group by key
		group->expr = as->l;

		// replace the returning expression with a reference to the group by key
		as->l = &group->ast;
	}
}

static void
parse_select_resolve_order_by(Stmt* self, AstSelect* select)
{
	auto node = select->expr_order_by.list;
	while (node)
	{
		auto order = ast_order_of(node->ast);

		if (order->expr->id == KINT)
		{
			// SELECT expr, ... ORDER BY int

			// find column in the select returning set (order starts from 1)
			auto pos = order->expr->integer;
			auto expr = returning_find(&select->ret, pos);
			if (! expr)
				stmt_error(self, order->expr, "column %d is not in the SELECT expr list", pos);
			// replace order by <int> to the select expression
			order->expr = expr->l;
		} else
		if (order->expr->id == KNAME)
		{
			// find column alias in select returning set
			auto ref      = &order->expr->string;
			bool conflict = false;
			auto column   = columns_find_noconflict(&select->ret.columns, ref, &conflict);
			if (column)
			{
				// replace order by <name> to the select expression
				auto expr = returning_find(&select->ret, column->order + 1);
				assert(expr);
				order->expr = expr->l;
			}
		}

		node = node->next;
	}
}

void
parse_select_resolve(Stmt* self)
{
	// resolve selects backwards
	for (auto ref = self->select_list.list_tail; ref; ref = ref->prev)
	{
		auto select = ast_select_of(ref->ast);

		// create select expressions and resolve *
		parse_returning_resolve(&select->ret, self, &select->targets);

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

		// resolve GROUP BY alias/int cases and create columns
		if (! targets_empty(&select->targets_group))
		{
			parse_select_resolve_group_by_alias(self, select);
			parse_select_resolve_group_by(select);
		}

		// resolve ORDER BY alias/int cases
		if (select->expr_order_by.count > 0)
			parse_select_resolve_order_by(self, select);
	}
}
