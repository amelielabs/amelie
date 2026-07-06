
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
#include <amelie_db>
#include <amelie_repl>
#include <amelie_vm>
#include <amelie_parser.h>
#include <amelie_plan.h>

static inline void
plan_expr(Plan* self)
{
	// SELECT expr
	plan_add_expr(self);
}

static inline void
plan_expr_set(Plan* self)
{
	// SELECT expr, ...
	plan_add_expr_set(self);
}

static inline void
plan_group_by(Plan* self)
{
	// SELECT FROM GROUP BY
	auto select = self->select;

	// SCAN_AGGS
	plan_add_scan_aggs(self);

	// SET_AGG_MERGE
	plan_add_set_agg_merge(self);

	// PIPE (group_target->r = aggs)
	plan_add_pipe(self);

	// SCAN (scan aggs and emit exprs)
	plan_add_scan(self,
	              select->expr_limit,
	              select->expr_offset,
	              select->expr_having,
	              &select->from_group);
}

static inline void
plan_group_by_order_by(Plan* self)
{
	// SELECT FROM GROUP BY ORDER BY
	auto select = self->select;

	// SCAN_AGGS
	plan_add_scan_aggs(self);

	// SET_AGG_MERGE
	plan_add_set_agg_merge(self);

	// PIPE (group_target->r = aggs)
	plan_add_pipe(self);

	// SCAN_ORDERED (scan aggs and emit exprs)
	plan_add_scan_ordered(self, NULL, NULL, select->expr_having,
	                      &select->from_group);

	// SORT
	plan_add_sort(self);

	// no distinct/limit/offset (return ordered set)
	if (select->distinct    == false &&
	    select->expr_limit  == NULL  &&
	    select->expr_offset == NULL)
		return;

	// UNION
	plan_add_union(self);
}

static inline void
plan_order_by(Plan* self)
{
	// SELECT FROM ORDER BY
	auto select = self->select;

	// SCAN_ORDERED
	plan_add_scan_ordered(self, NULL, NULL, select->expr_where,
	                      &select->from);

	// SORT
	plan_add_sort(self);

	// no distinct/limit/offset (return ordered set)
	if (select->distinct    == false &&
	    select->expr_limit  == NULL  &&
	    select->expr_offset == NULL)
		return;

	// UNION
	plan_add_union(self);
}

static inline void
plan_scan(Plan* self)
{
	// SELECT FROM
	// SELECT FROM JOIN ON
	auto select = self->select;

	// SCAN (table/expression and joins)
	plan_add_scan(self,
	              select->expr_limit,
	              select->expr_offset,
	              select->expr_where,
	              &select->from);
}

static void
plan_main(Plan* self, bool emit_store)
{
	// SELECT expr[, ...]
	auto select     = self->select;
	auto from       = &select->from;
	auto from_group = &select->from_group;

	if (from_empty(from))
	{
		if (emit_store || select->ret.count > 1)
			plan_expr_set(self);
		else
			plan_expr(self);
		return;
	}

	// SELECT FROM GROUP BY [WHERE] [HAVING] [ORDER BY] [LIMIT/OFFSET]
	// SELECT aggregate FROM
	auto order_by = &select->order_by;
	if (! from_empty(from_group))
	{
		if (order_by->count > 0)
		{
			plan_group_by_order_by(self);
			return;
		}
		plan_group_by(self);
		return;
	}

	// SELECT FROM [WHERE] ORDER BY [LIMIT/OFFSET]
	// SELECT DISTINCT FROM
	if (order_by->count > 0)
	{
		// use plain scan if the order by matches the table index keys
		auto first = from_first(from);
		if (!from_is_join(from) && !from_is_expr(from))
		{
			auto index = first->from_index;
			if (index->type == INDEX_TREE && ast_order_list_match_index(order_by, first))
			{
				plan_scan(self);
				return;
			}
		}

		plan_order_by(self);
		return;
	}

	// SELECT FROM [WHERE] LIMIT/OFFSET
	// SELECT FROM [WHERE]
	plan_scan(self);
}

Plan*
plan_create(Ast* ast, bool emit_store)
{
	auto select = ast_select_of(ast);
	auto self = (Plan*)palloc(sizeof(Plan));
	self->select = select;
	self->r      = -1;
	self->recv   = false;
	list_init(&self->list);
	list_init(&self->list_recv);

	// prepare scan path using where expression per target
	//
	// path created only for tables
	//
	path_prepare(&select->from, select->expr_where);

	if (select->pushdown)
		plan_pushdown(self);
	else
		plan_main(self, emit_store);
	return self;
}
