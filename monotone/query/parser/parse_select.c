
//
// monotone
//
// SQL OLTP database
//

#include <monotone_runtime.h>
#include <monotone_io.h>
#include <monotone_data.h>
#include <monotone_lib.h>
#include <monotone_config.h>
#include <monotone_auth.h>
#include <monotone_schema.h>
#include <monotone_transaction.h>
#include <monotone_storage.h>
#include <monotone_wal.h>
#include <monotone_db.h>
#include <monotone_value.h>
#include <monotone_aggr.h>
#include <monotone_request.h>
#include <monotone_vm.h>
#include <monotone_parser.h>

static inline void
parse_select_group_by(Stmt* self, AstList* list)
{
	for (;;)
	{
		auto expr = parse_expr(self, NULL);

		// [AS label]
		Ast* label = NULL;
		if (stmt_if(self, KAS))
		{
			// name
			auto name = stmt_next(self);
			if (name->id != KNAME && name->id != KNAME_COMPOUND)
				error("GROUP BY AS <name> expected");
			label = name;
		} else
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
parse_select_order_by(Stmt* self, AstList* list)
{
	for (;;)
	{
		// expr
		auto expr = parse_expr(self, NULL);

		// type
		int type = 0;
		auto ast = stmt_next(self);
		if (ast->id == KTINT)
			type = TYPE_INT;
		else
		if (ast->id == KTSTRING)
			type = TYPE_STRING;
		else
			error("ORDER BY type must be int or string");

		// [ASC|DESC]
		bool asc = true;
		if (stmt_if(self, KASC))
			asc = true;
		else
		if (stmt_if(self, KDESC))
			asc = false;

		auto order = ast_order_allocate(expr, type, asc);
		ast_list_add(list, &order->ast);

		// ,
		if (stmt_if(self, ','))
			continue;

		break;
	}
}

hot AstSelect*
parse_select(Stmt* self)
{
	// SELECT expr, ... [FROM name, [...]]
	// [GROUP BY]
	// [WHERE expr]
	// [ORDER BY]
	// [LIMIT expr] [OFFSET expr]
	int level = target_list_next_level(&self->target_list);
	auto select = ast_select_allocate();

	// expr, ...
	Ast* expr_prev = NULL;
	for (;;)
	{
		Expr ctx;
		expr_init(&ctx);
		ctx.aggs = &select->expr_aggs;
		ctx.aggs_global = &self->aggr_list;

		auto expr = parse_expr(self, &ctx);
		if (select->expr == NULL)
			select->expr = expr;
		else
			expr_prev->next = expr;
		expr_prev = expr;
		select->expr_count++;

		// ,
		if (stmt_if(self, ','))
			continue;
		break;
	}

	// [FROM]
	if (stmt_if(self, KFROM))
		select->target = parse_from(self, level);

	/* [WHERE] */
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

		parse_select_group_by(self, &select->expr_group_by);

		// [HAVING]
		if (stmt_if(self, KHAVING))
			select->expr_having = parse_expr(self, NULL);
	}

	// add group by target
	if (select->expr_group_by.count > 0 || select->expr_aggs.count > 0)
	{
		if (select->target == NULL)
			error("no targets to use with GROUP BY or aggregates");

		// set aggregates target
		if (select->expr_aggs.count)
		{
			select->target->aggs = select->expr_aggs;
			ast_aggr_set_target(&select->expr_aggs, select->target);
		}

		select->target_group =
			target_list_add(&self->target_list, level, 0, NULL, NULL, NULL);

		select->target_group->group_redirect = select->target_group;
		select->target_group->group_main     = select->target;
		select->target->group_by_target      = select->target_group;
		select->target->group_by             = select->expr_group_by;
	}

	// [ORDER BY]
	if (stmt_if(self, KORDER))
	{
		if (! stmt_if(self, KBY))
			error("ORDER <BY> expected");

		parse_select_order_by(self, &select->expr_order_by);
	}

	// [LIMIT]
	if (stmt_if(self, KLIMIT))
		select->expr_limit = parse_expr(self, NULL);

	// [OFFSET]
	if (stmt_if(self, KOFFSET))
		select->expr_offset = parse_expr(self, NULL);

	return select;
}
