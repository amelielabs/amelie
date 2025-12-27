
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
#include <amelie_plan.h>

static void
plan_pushdown_group_by(Plan* self)
{
	// SELECT FROM GROUP BY
	auto select = self->select;

	// SCAN_AGGS
	plan_add_scan_aggs(self);

	// ----
	plan_switch(self, true);

	// RECV_AGGS
	plan_add_recv_aggs(self);

	// PIPE (group_target->r = set)
	plan_add_pipe(self);

	// SCAN (aggs and emit select exprs)
	plan_add_scan(self,
	              select->expr_limit,
	              select->expr_offset,
	              select->expr_having,
	              &select->from_group);
}

static void
plan_pushdown_group_by_order_by(Plan* self)
{
	// SELECT FROM GROUP BY ORDER BY
	auto select = self->select;

	// SCAN_AGGS
	plan_add_scan_aggs(self);

	// ----
	plan_switch(self, true);

	// RECV_AGGS
	plan_add_recv_aggs(self);

	// PIPE (group_target->r = set)
	plan_add_pipe(self);

	// SCAN_ORDERED (aggs and emit select exprs)
	plan_add_scan_ordered(self, NULL, NULL, select->expr_having,
	                      &select->from_group);

	// SORT
	plan_add_sort(self);

	// no distinct/limit/offset (return set)
	if (select->distinct    == false &&
	    select->expr_limit  == NULL  &&
	    select->expr_offset == NULL)
		return;

	// UNION
	plan_add_union(self);
}

static void
plan_pushdown_order_by(Plan* self)
{
	// SELECT FROM ORDER BY
	auto select = self->select;

	// SCAN_ORDERED (table scan)
	plan_add_scan_ordered(self, NULL, NULL, select->expr_where,
	                      &select->from);

	// SORT
	plan_add_sort(self);

	// ----
	plan_switch(self, true);

	// RECV_ORDERED
	plan_add_recv_ordered(self);
}

static void
plan_pushdown_order_by_index(Plan* self)
{
	// SELECT FROM ORDER BY
	auto select = self->select;

	// pushdown limit
	//
	// push limit as limit = limit + offset
	Ast* limit = NULL;
	if (select->expr_limit && select->expr_offset)
	{
		limit = ast('+');
		limit->l = select->expr_limit;
		limit->r = select->expr_offset;
	} else
	if (select->expr_limit) {
		limit = select->expr_limit;
	}

	// SCAN_ORDERED (table scan)
	plan_add_scan_ordered(self, limit, NULL, select->expr_where,
	                      &select->from);

	// the result set is ordered by index, no explicit
	// SORT command needed

	// ----
	plan_switch(self, true);

	// RECV
	plan_add_recv_ordered(self);
}

static void
plan_pushdown_scan(Plan* self)
{
	// SELECT FROM
	auto select = self->select;

	// push limit as limit = limit + offset
	Ast* limit = NULL;
	if (select->expr_limit && select->expr_offset)
	{
		limit = ast('+');
		limit->l = select->expr_limit;
		limit->r = select->expr_offset;
	} else
	if (select->expr_limit) {
		limit = select->expr_limit;
	}

	// SCAN
	plan_add_scan(self, limit, NULL, select->expr_where,
	              &select->from);

	// ----
	plan_switch(self, true);

	// RECV
	plan_add_recv(self);
}

void
plan_pushdown(Plan* self)
{
	// SELECT FROM GROUP BY [WHERE] [HAVING] [ORDER BY] [LIMIT/OFFSET]
	// SELECT aggregate FROM
	auto select     = self->select;
	auto from       = &select->from;
	auto from_group = &select->from_group;

	auto order_by = &select->order_by;
	if (! from_empty(from_group))
	{
		if (order_by->count > 0)
		{
			plan_pushdown_group_by_order_by(self);
			return;
		}

		plan_pushdown_group_by(self);
		return;
	}

	// SELECT FROM [WHERE] ORDER BY [LIMIT/OFFSET]
	// SELECT DISTINCT FROM
	if (order_by->count > 0)
	{
		// optimize scan if the order by matches the table index keys
		auto first = from_first(from);
		if (! from_is_join(from))
		{
			auto index = first->from_index;
			if (index->type == INDEX_TREE && ast_order_list_match_index(order_by, first))
			{
				plan_pushdown_order_by_index(self);
				return;
			}
		}

		plan_pushdown_order_by(self);
		return;
	}

	// SELECT FROM [WHERE] ORDER BY [LIMIT/OFFSET]
	// SELECT FROM [WHERE] LIMIT/OFFSET
	// SELECT FROM [WHERE]
	plan_pushdown_scan(self);
}
