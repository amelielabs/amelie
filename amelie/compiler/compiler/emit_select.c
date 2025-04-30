
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
#include <amelie_compiler.h>

static inline void
emit_select_expr(Compiler* self, Targets* targets, AstSelect* select)
{
	// push expr and prepare returning columns
	for (auto as = select->ret.list; as; as = as->next)
	{
		auto column = as->r->column;
		// expr
		int rexpr = emit_expr(self, targets, as->l);
		int rt = rtype(self, rexpr);
		column_set_type(column, rt, type_sizeof(rt));
		op1(self, CPUSH, rexpr);
		runpin(self, rexpr);
	}
}

void
emit_select_on_match(Compiler* self, Targets* targets, void* arg)
{
	AstSelect* select = arg;

	// push expressions
	emit_select_expr(self, targets, select);

	// push order by key (if any)
	auto node = select->expr_order_by.list;
	while (node)
	{
		auto order = ast_order_of(node->ast);
		int rexpr_order_by;
		rexpr_order_by = emit_expr(self, targets, order->expr);
		op1(self, CPUSH, rexpr_order_by);
		runpin(self, rexpr_order_by);
		node = node->next;
	}

	// add to the returning set
	op1(self, CSET_ADD, select->rset);
}

void
emit_select_on_match_aggregate(Compiler* self, Targets* targets, void* arg)
{
	AstSelect* select = arg;

	// create a list of aggs based on type
	select->aggs = code_data_pos(self->code_data);
	buf_claim(&self->code_data->data, sizeof(int) * select->expr_aggs.count);

	// get existing or create a new row by key, return
	// the row reference

	// push group by keys
	auto node = select->expr_group_by.list;
	for (; node; node = node->next)
	{
		auto group = ast_group_of(node->ast);
		// expr
		auto rexpr = emit_expr(self, targets, group->expr);
		auto rt = rtype(self, rexpr);
		column_set_type(group->column, rt, type_sizeof(rt));
		op1(self, CPUSH, rexpr);
		runpin(self, rexpr);
	}

	// CSET_GET
	select->rset_agg_row =
		op2(self, CSET_GET, rpin(self, TYPE_INT), select->rset_agg);

	// push aggs
	node = select->expr_aggs.list;
	for (; node; node = node->next)
	{
		auto agg = ast_agg_of(node->ast);
		agg->select = &select->ast;

		// expr
		auto rexpr = emit_expr(self, targets, agg->expr);
		auto rt = rtype(self, rexpr);
		column_set_type(agg->column, rt, type_sizeof(rt));
		op1(self, CPUSH, rexpr);
		runpin(self, rexpr);

		// lambda
		int* aggs = (int*)code_data_at(self->code_data, select->aggs);
		if (! agg->function)
		{
			if (rt != agg->expr_seed_type)
				stmt_error(self->current, agg->expr, "lambda expression type mismatch");
			agg->id = AGG_LAMBDA;
			aggs[agg->order] = AGG_LAMBDA;
			continue;
		}

		switch (agg->function->id) {
		case KCOUNT:
			if (agg->distinct)
				agg->id = AGG_INT_COUNT_DISTINCT;
			else
				agg->id = AGG_INT_COUNT;
			break;
		case KMIN:
			if (rt == TYPE_INT || rt == TYPE_NULL)
				agg->id = AGG_INT_MIN;
			else
			if (rt == TYPE_DOUBLE)
				agg->id = AGG_DOUBLE_MIN;
			else
				stmt_error(self->current, agg->expr, "int or double expected");
			break;
		case KMAX:
			if (rt == TYPE_INT || rt == TYPE_NULL)
				agg->id = AGG_INT_MAX;
			else
			if (rt == TYPE_DOUBLE)
				agg->id = AGG_DOUBLE_MAX;
			else
				stmt_error(self->current, agg->expr, "int or double expected");
			break;
		case KSUM:
			if (rt == TYPE_INT || rt == TYPE_NULL)
				agg->id = AGG_INT_SUM;
			else
			if (rt == TYPE_DOUBLE)
				agg->id = AGG_DOUBLE_SUM;
			else
				stmt_error(self->current, agg->expr, "int or double expected");
			break;
		case KAVG:
			if (rt == TYPE_INT || rt == TYPE_NULL)
				agg->id = AGG_INT_AVG;
			else
			if (rt == TYPE_DOUBLE)
				agg->id = AGG_DOUBLE_AVG;
			else
				stmt_error(self->current, agg->expr, "int or double expected");
			break;
		}
		aggs[agg->order] = agg->id;
	}

	// CSET_AGG
	if (select->expr_aggs.count > 0)
		op3(self, CSET_AGG, select->rset_agg,
		    select->rset_agg_row,
		    select->aggs);

	runpin(self, select->rset_agg_row);
	select->rset_agg_row = -1;
}

void
emit_select_on_match_aggregate_empty(Compiler* self, Targets* targets, void* arg)
{
	// process NULL values for the aggregate
	AstSelect* select = arg;

	// push group by keys
	auto node = select->expr_group_by.list;
	while (node)
	{
		auto group = ast_group_of(node->ast);
		auto rexpr = emit_expr(self, targets, group->expr);
		auto rt = rtype(self, rexpr);
		column_set_type(group->column, rt, type_sizeof(rt));
		op1(self, CPUSH, rexpr);
		runpin(self, rexpr);
		node = node->next;
	}

	// CSET_GET
	select->rset_agg_row =
		op2(self, CSET_GET, rpin(self, TYPE_INT), select->rset_agg);

	// push aggs
	node = select->expr_aggs.list;
	while (node)
	{
		auto agg = ast_agg_of(node->ast);
		auto rexpr = op1(self, CNULL, rpin(self, TYPE_NULL));
		auto rt = rtype(self, rexpr);
		column_set_type(agg->column, rt, type_sizeof(rt));
		op1(self, CPUSH, rexpr);
		runpin(self, rexpr);
		node = node->next;
	}

	// CSET_AGG
	if (select->expr_aggs.count > 0)
		op3(self, CSET_AGG, select->rset_agg,
		    select->rset_agg_row,
		    select->aggs);

	runpin(self, select->rset_agg_row);
	select->rset_agg_row = -1;
}

int
emit_select_order_by_data(Compiler* self, AstSelect* select, bool use_group_by)
{
	int order_offset = code_data_pos(self->code_data);
	if (use_group_by)
	{
		auto  size  = sizeof(bool) * select->expr_group_by.count;
		bool* order = buf_claim(&self->code_data->data, size);
		memset(order, true, size);
	} else
	{
		// write order by asc/desc flags
		bool* order = buf_claim(&self->code_data->data, sizeof(bool) * select->expr_order_by.count);
		auto  node = select->expr_order_by.list;
		while (node)
		{
			auto ref = ast_order_of(node->ast);
			order[ref->order] = ref->asc;
			node = node->next;
		}
	}
	return order_offset;
}

hot int
emit_select_union(Compiler* self, AstSelect* select)
{
	// distinct
	int rdistinct = op2(self, CBOOL, rpin(self, TYPE_BOOL), select->distinct);
	op1(self, CPUSH, rdistinct);
	runpin(self, rdistinct);

	// limit
	int rlimit = -1;
	if (select->expr_limit)
		rlimit = emit_expr(self, &select->targets, select->expr_limit);

	// offset
	int roffset = -1;
	if (select->expr_offset)
		roffset = emit_expr(self, &select->targets, select->expr_offset);

	// CUNION
	int runion = op4(self, CUNION, rpin(self, TYPE_STORE), select->rset,
	                 rlimit, roffset);

	runpin(self, select->rset);
	select->rset = -1;
	if (rlimit != -1)
		runpin(self, rlimit);
	if (roffset != -1)
		runpin(self, roffset);

	return runion;
}

hot static void
emit_select_group_by_scan(Compiler* self, AstSelect* select,
                          Ast*      limit,
                          Ast*      offset)
{
	// create agg set
	int rset;
	rset = op3(self, CSET, rpin(self, TYPE_STORE),
	           select->expr_aggs.count,
	           select->expr_group_by.count);
	select->rset_agg = rset;

	// emit aggs seed expressions
	auto node = select->expr_aggs.list;
	for (; node; node = node->next)
	{
		auto agg = ast_agg_of(node->ast);
		if (! agg->expr_seed)
			continue;
		agg->rseed = emit_expr(self, select->targets.outer, agg->expr_seed);
		agg->expr_seed_type = rtype(self, agg->rseed);
	}

	// scan over target and process aggregates per group by key
	scan(self, &select->targets,
	     NULL,
	     NULL,
	     select->expr_where,
	     emit_select_on_match_aggregate,
	     select);

	// select aggr [without group by]
	//
	// force create empty record by processing one NULL value
	if (! select->expr_group_by_has)
		emit_select_on_match_aggregate_empty(self, &select->targets, select);

	// free seed values
	node = select->expr_aggs.list;
	for (; node; node = node->next)
	{
		auto agg = ast_agg_of(node->ast);
		if (! agg->expr_seed)
			continue;
		op1(self, CFREE, agg->rseed);
		runpin(self, agg->rseed);
		agg->rseed = -1;
	}

	// scan over created group
	//
	// result will be added to the set
	//
	auto target_group = targets_outer(&select->targets_group);
	target_group->r = rset;

	scan(self,
	     &select->targets_group,
	     limit,
	     offset,
	     select->expr_having,
	     emit_select_on_match,
	     select);

	runpin(self, select->rset_agg);
	select->rset_agg = -1;
	target_group->r = -1;
}

hot static int
emit_select_group_by(Compiler* self, AstSelect* select)
{
	//
	// SELECT expr, ... [FROM name, [...]]
	// GROUP BY expr, ...
	// [WHERE expr]
	// [ORDER BY]
	// [LIMIT expr] [OFFSET expr]
	//
	int rresult = -1;
	if (select->expr_order_by.count == 0)
	{
		// create result set
		select->rset = op3(self, CSET, rpin(self, TYPE_STORE), select->ret.count, 0);
		rresult = select->rset;

		// generate group by scan using limit/offset
		emit_select_group_by_scan(self, select, select->expr_limit,
		                          select->expr_offset);
	} else
	{
		// write order by key types
		int offset = emit_select_order_by_data(self, select, false);
		select->rset = op4(self, CSET_ORDERED, rpin(self, TYPE_STORE),
		                   select->ret.count,
		                   select->expr_order_by.count,
		                   offset);

		// generate group by scan
		emit_select_group_by_scan(self, select, NULL, NULL);

		// sort select set
		op1(self, CSET_SORT, select->rset);

		// create union object and add sorted set, apply limit/offset
		rresult = emit_select_union(self, select);
	}
	return rresult;
}

hot static int
emit_select_order_by(Compiler* self, AstSelect* select)
{
	//
	// SELECT expr, ... [FROM name, [...]] [WHERE expr]
	// ORDER BY
	// [LIMIT expr] [OFFSET expr]
	//

	// create ordered set
	int offset = emit_select_order_by_data(self, select, false);

	select->rset = op4(self, CSET_ORDERED, rpin(self, TYPE_STORE),
	                   select->ret.count,
	                   select->expr_order_by.count,
	                   offset);

	// scan for table/expression and joins
	scan(self,
	     &select->targets,
	     NULL,
	     NULL,
	     select->expr_where,
	     emit_select_on_match,
	     select);

	// sort select set
	op1(self, CSET_SORT, select->rset);

	// create union object and add sorted set
	return emit_select_union(self, select);
}

hot static int
emit_select_scan(Compiler* self, AstSelect* select)
{
	//
	// SELECT expr, ... [FROM name, [...]]
	// [WHERE expr]
	// [LIMIT expr] [OFFSET expr]
	//

	// create result set
	int rresult = op3(self, CSET, rpin(self, TYPE_STORE), select->ret.count, 0);
	select->rset = rresult;

	// scan for table/expression and joins
	scan(self,
	     &select->targets,
	     select->expr_limit,
	     select->expr_offset,
	     select->expr_where,
	     emit_select_on_match,
	     select);

	return select->rset;
}

hot int
emit_select(Compiler* self, Ast* ast)
{
	AstSelect* select = ast_select_of(ast);

	// SELECT expr[, ...]
	if (targets_empty(&select->targets))
	{
		// create result set
		int rresult = op3(self, CSET, rpin(self, TYPE_STORE), select->ret.count, 0);
		select->rset = rresult;

		// push expressions
		emit_select_expr(self, &select->targets, select);

		// add to the returning set
		op1(self, CSET_ADD, select->rset);
		return rresult;
	}
	//
	// SELECT expr, ... [FROM name, [...]]
	// [GROUP BY]
	// [WHERE expr]
	// [ORDER BY]
	// [LIMIT expr] [OFFSET expr]
	//

	// SELECT FROM GROUP BY [WHERE] [HAVING] [ORDER BY] [LIMIT/OFFSET]
	// SELECT aggregate FROM
	if (! targets_empty(&select->targets_group))
		return emit_select_group_by(self, select);

	// SELECT FROM [WHERE] ORDER BY [LIMIT/OFFSET]
	// SELECT DISTINCT FROM
	if (select->expr_order_by.count > 0)
		return emit_select_order_by(self, select);

	// SELECT FROM [WHERE] LIMIT/OFFSET
	// SELECT FROM [WHERE]
	return emit_select_scan(self, select);
}
