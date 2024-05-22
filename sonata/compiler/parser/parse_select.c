
//
// sonata.
//
// SQL Database for JSON.
//

#include <sonata_runtime.h>
#include <sonata_io.h>
#include <sonata_lib.h>
#include <sonata_data.h>
#include <sonata_config.h>
#include <sonata_auth.h>
#include <sonata_http.h>
#include <sonata_client.h>
#include <sonata_server.h>
#include <sonata_def.h>
#include <sonata_transaction.h>
#include <sonata_index.h>
#include <sonata_storage.h>
#include <sonata_wal.h>
#include <sonata_db.h>
#include <sonata_value.h>
#include <sonata_aggr.h>
#include <sonata_executor.h>
#include <sonata_vm.h>
#include <sonata_parser.h>

static inline void
parse_select_group_by(Stmt* self, AstSelect* select)
{
	auto list = &select->expr_group_by;
	for (;;)
	{
		auto expr = parse_expr(self, NULL);

		// [AS name]
		// [name]
		Ast* label = NULL;
		if (stmt_if(self, KAS))
		{
			label = stmt_if(self, KNAME);
			if (unlikely(! label))
				error("AS <label> expected");
		} else {
			label = stmt_if(self, KNAME);
		}

		if (! label)
		{
			if (expr->id == KNAME ||
			    expr->id == KNAME_COMPOUND)
			{
				label = expr;
			} else {
				// label is required for expressions
			}
		}

		auto group = ast_group_allocate(list->count, expr, label);
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
	Expr ctx;
	expr_init(&ctx);
	ctx.aggs = &select->expr_aggs;

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

		auto order = ast_order_allocate(expr, asc);
		ast_list_add(&select->expr_order_by, &order->ast);

		// ,
		if (stmt_if(self, ','))
			continue;

		break;
	}
}

static inline void
parse_select_distinct(Stmt* self, AstSelect* select)
{
	select->distinct            = true;
	select->distinct_expr       = NULL;
	select->distinct_expr_count = 0;

	// [ON]
	if (! stmt_if(self, KON))
		return;

	// (
	if (! stmt_if(self, '('))
		error("DISTINCT <(> expected");

	// (expr, ...)
	Expr ctx;
	expr_init(&ctx);
	ctx.aggs = &select->expr_aggs;

	Ast* expr_prev = NULL;
	for (;;)
	{
		// expr
		auto expr = parse_expr(self, &ctx);
		if (select->distinct_expr == NULL)
			select->distinct_expr = expr;
		else
			expr_prev->next = expr;
		expr_prev = expr;
		select->distinct_expr_count++;

		// set distinct expr as order by key
		auto order = ast_order_allocate(expr, true);
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

static inline void
parse_select_label(Stmt* self, AstSelect* select, Ast* expr)
{
	// [AS name]
	// [name]
	Ast* name = NULL;
	if (stmt_if(self, KAS))
	{
		name = stmt_if(self, KNAME);
		if (unlikely(! name))
			error("AS <label> expected");
	} else {
		name = stmt_if(self, KNAME);
	}
	if (! name)
		return;

	// ensure label does not exists
	auto label = ast_label_match(&select->expr_labels, &name->string);
	if (unlikely(label))
		error("label <%.*s> redefined", str_size(&name->string),
		      str_of(&name->string));

	// create label
	label = ast_label_allocate(name, expr, select->expr_count);
	ast_list_add(&select->expr_labels, &label->ast);
}

hot AstSelect*
parse_select(Stmt* self)
{
	// SELECT [DISTINCT] expr, ... [FROM name, [...]]
	// [GROUP BY]
	// [WHERE expr]
	// [ORDER BY]
	// [LIMIT expr] [OFFSET expr]
	int level = target_list_next_level(&self->target_list);
	auto select = ast_select_allocate();

	// [DISTINCT]
	if (stmt_if(self, KDISTINCT))
		parse_select_distinct(self, select);

	// expr, ...
	Expr ctx;
	expr_init(&ctx);
	ctx.aggs = &select->expr_aggs;

	Ast* expr_prev = NULL;
	for (;;)
	{
		auto expr = parse_expr(self, &ctx);
		if (select->expr == NULL)
			select->expr = expr;
		else
			expr_prev->next = expr;
		expr_prev = expr;

		// [AS name]
		// [name]
		parse_select_label(self, select, expr);
		select->expr_count++;

		// ,
		if (stmt_if(self, ','))
			continue;
		break;
	}

	// use select expr as distinct expression, if not set
	if (select->distinct && !select->distinct_expr_count)
	{
		select->distinct_expr = select->expr;
		select->distinct_expr_count = select->expr_count;

		// set distinct expressions as order by keys
		for (auto expr = select->expr; expr; expr = expr->next)
		{
			auto order = ast_order_allocate(expr, true);
			ast_list_add(&select->expr_order_by, &order->ast);
		}
	}

	// [FROM]
	if (stmt_if(self, KFROM))
	{
		select->target = parse_from(self, level);
		select->target->labels = &select->expr_labels;
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

		select->target_group =
			target_list_add(&self->target_list, level, 0, NULL, NULL, NULL, NULL);

		select->target_group->group_redirect = select->target_group;
		select->target_group->group_main     = select->target;
		select->target->group_by_target      = select->target_group;
		select->target->group_by             = select->expr_group_by;
	}

	return select;
}
