
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

#include <amelie_base.h>
#include <amelie_os.h>
#include <amelie_lib.h>
#include <amelie_json.h>
#include <amelie_runtime.h>
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
#include <amelie_catalog.h>
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

	// select agg(distinct)
	if (select->distinct_aggs)
	{
		// SCAN_AGGS_ORDERED
		plan_add_scan_aggs_ordered(self);

		// SORT
		plan_add_sort(self);

		// UNION_AGGS
		plan_add_union_aggs(self);
	} else
	{
		// SCAN_AGGS
		plan_add_scan_aggs(self);
	}

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

	// select agg(distinct)
	if (select->distinct_aggs)
	{
		// SCAN_AGGS_ORDERED
		plan_add_scan_aggs_ordered(self);

		// SORT
		plan_add_sort(self);

		// UNION_AGGS
		plan_add_union_aggs(self);
	} else
	{
		// SCAN_AGGS
		plan_add_scan_aggs(self);
	}

	// PIPE (group_target->r = aggs)
	plan_add_pipe(self);

	// SCAN_ORDERED (scan aggs and emit exprs)
	plan_add_scan_ordered(self, NULL, NULL, select->expr_having,
	                      &select->from_group);

	// SORT
	plan_add_sort(self);

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
	auto order_by = &select->expr_order_by;
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
