
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
#include <amelie_tier>
#include <amelie_storage>
#include <amelie_repl>
#include <amelie_vm>
#include <amelie_parser.h>
#include <amelie_plan.h>
#include <amelie_compiler.h>

static void
on_match(Scan* self)
{
	auto  cp     = self->compiler;
	Plan* plan   = self->on_match_arg;
	auto  select = plan->select;

	// push expressions and set columns
	for (auto as = select->ret.list; as; as = as->next)
	{
		auto column = as->r->column;
		auto type = emit_push(cp, self->from, as->l);
		column_set_type(column, type, type_sizeof(type));
	}

	// push order by key (if any)
	auto node = select->order_by.list;
	for (; node; node = node->next)
		emit_push(cp, self->from, ast_order_of(node->ast)->expr);

	// add to the returning set
	op1(cp, CSET_ADD, plan->r);
}

static void
on_match_aggs(Scan* self)
{
	auto  cp     = self->compiler;
	Plan* plan   = self->on_match_arg;
	auto  select = plan->select;

	// create a list of aggs based on type
	select->aggs = code_data_pos(cp->code_data);
	buf_emplace(&cp->code_data->data, sizeof(Agg) * select->expr_aggs.count);

	// get existing or create a new row by key, return
	// the row reference

	// push group by keys
	auto node = select->group_by.list;
	for (; node; node = node->next)
	{
		auto group = ast_group_of(node->ast);
		auto type = emit_push(cp, self->from, group->expr);
		column_set_type(group->column, type, type_sizeof(type));
	}

	// CSET_GET
	select->rset_agg_row = op2pin(cp, CSET_GET, TYPE_INT, select->rset_agg);

	// push aggs
	node = select->expr_aggs.list;
	for (; node; node = node->next)
	{
		auto agg = ast_agg_of(node->ast);
		agg->select = &select->ast;

		// push expr and set type
		auto rt = emit_push(cp, self->from, agg->expr);
		column_set_type(agg->column, rt, type_sizeof(rt));

		// lambda
		auto aggs = (Agg*)code_data_at(cp->code_data, select->aggs);
		aggs[agg->order].distinct = agg->distinct;
		if (! agg->function)
		{
			if (rt != agg->expr_seed_type)
				stmt_error(cp->current, agg->expr, "lambda expression type mismatch");
			agg->id = AGG_LAMBDA;
			aggs[agg->order].type = AGG_LAMBDA;
			continue;
		}

		switch (agg->function->id) {
		case KCOUNT:
			agg->id = AGG_INT_COUNT;
			break;
		case KMIN:
			if (rt == TYPE_INT || rt == TYPE_NULL)
				agg->id = AGG_INT_MIN;
			else
			if (rt == TYPE_DOUBLE)
				agg->id = AGG_DOUBLE_MIN;
			else
				stmt_error(cp->current, agg->expr, "int or double expected");
			break;
		case KMAX:
			if (rt == TYPE_INT || rt == TYPE_NULL)
				agg->id = AGG_INT_MAX;
			else
			if (rt == TYPE_DOUBLE)
				agg->id = AGG_DOUBLE_MAX;
			else
				stmt_error(cp->current, agg->expr, "int or double expected");
			break;
		case KSUM:
			if (rt == TYPE_INT || rt == TYPE_NULL)
				agg->id = AGG_INT_SUM;
			else
			if (rt == TYPE_DOUBLE)
				agg->id = AGG_DOUBLE_SUM;
			else
				stmt_error(cp->current, agg->expr, "int or double expected");
			break;
		case KAVG:
			if (rt == TYPE_INT || rt == TYPE_NULL)
				agg->id = AGG_INT_AVG;
			else
			if (rt == TYPE_DOUBLE)
				agg->id = AGG_DOUBLE_AVG;
			else
				stmt_error(cp->current, agg->expr, "int or double expected");
			break;
		}
		aggs[agg->order].type = agg->id;
	}

	// CSET_AGG
	if (select->expr_aggs.count > 0)
		op3(cp, CSET_AGG, select->rset_agg,
		    select->rset_agg_row,
		    select->aggs);

	runpin(cp, select->rset_agg_row);
	select->rset_agg_row = -1;
}

static void
on_match_aggs_empty(Compiler* self, AstSelect* select)
{
	// process NULL values for the aggregate
	auto from = &select->from;

	// push group by keys
	auto node = select->group_by.list;
	while (node)
	{
		auto group = ast_group_of(node->ast);
		auto type = emit_push(self, from, group->expr);
		column_set_type(group->column, type, type_sizeof(type));
		node = node->next;
	}

	// CSET_GET
	select->rset_agg_row = op2pin(self, CSET_GET, TYPE_INT, select->rset_agg);

	// push aggs
	node = select->expr_aggs.list;
	while (node)
	{
		auto agg = ast_agg_of(node->ast);
		op1(self, CPUSH_NULLS, 1);
		column_set_type(agg->column, TYPE_NULL, -1);
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

static int
emit_order(Compiler* self, AstSelect* select, bool use_group_by)
{
	int order_offset = code_data_pos(self->code_data);
	if (use_group_by)
	{
		auto size  = sizeof(bool) * select->group_by.count;
		auto order = (bool*)buf_emplace(&self->code_data->data, size);
		memset(order, true, size);
	} else
	{
		// write order by asc/desc flags
		auto order = (bool*)buf_emplace(&self->code_data->data, sizeof(bool) * select->order_by.count);
		auto node = select->order_by.list;
		while (node)
		{
			auto ref = ast_order_of(node->ast);
			order[ref->order] = ref->asc;
			node = node->next;
		}
	}
	return order_offset;
}

static void
cmd_expr(Compiler* self, Plan* plan, Command* ref)
{
	unused(ref);
	// push expr and prepare returning columns
	auto select = plan->select;
	auto as     = select->ret.list;
	auto column = as->r->column;
	// expr
	plan->r = emit_expr(self, &select->from, as->l);
	auto type = rtype(self, plan->r);
	column_set_type(column, type, type_sizeof(type));
}

static void
cmd_expr_set(Compiler* self, Plan* plan, Command* ref)
{
	unused(ref);
	auto select = plan->select;

	// create result set
	plan->r = op3pin(self, CSET, TYPE_STORE, select->ret.count, 0);

	// push expressions and set columns
	for (auto as = select->ret.list; as; as = as->next)
	{
		auto column = as->r->column;
		auto type = emit_push(self, &select->from, as->l);
		column_set_type(column, type, type_sizeof(type));
	}

	// add to the returning set
	op1(self, CSET_ADD, plan->r);
}

static void
cmd_scan(Compiler* self, Plan* plan, Command* ref)
{
	auto cmd = (CommandScan*)ref;

	// create result set
	auto select = plan->select;
	if (ref->id == COMMAND_SCAN_ORDERED)
	{
		auto offset = emit_order(self, select, false);
		plan->r = op4pin(self, CSET_ORDERED, TYPE_STORE,
		                 select->ret.count,
		                 select->order_by.count,
		                 offset);
	} else {
		plan->r = op3pin(self, CSET, TYPE_STORE,
		                 select->ret.count,
		                 0);
	}

	// scan for table/expression and joins
	scan(self,
	     cmd->from,
	     cmd->expr_limit,
	     cmd->expr_offset,
	     cmd->expr_where,
	     on_match,
	     plan);
}

static void
cmd_scan_aggs(Compiler* self, Plan* plan, Command* ref)
{
	unused(ref);
	auto select = plan->select;

	// create result agg set
	plan->r = op3pin(self, CSET, TYPE_STORE, select->expr_aggs.count,
	                 select->group_by.count);

	select->rset_agg = plan->r;

	// create and assign distinct set to the parent set to
	// process distinct aggregates
	if (select->distinct_aggs)
	{
		// [group_by_keys, expr, agg_order] = row_pos

		// CSET_DISTINCT
		op3(self, CSET_DISTINCT, select->rset_agg, 1, // row_pos
		    select->group_by.count + 1 + 1);
	}

	// emit aggs seed expressions
	auto node = select->expr_aggs.list;
	for (; node; node = node->next)
	{
		auto agg = ast_agg_of(node->ast);
		if (! agg->expr_seed)
			continue;
		agg->rseed = emit_expr(self, select->from.outer, agg->expr_seed);
		agg->expr_seed_type = rtype(self, agg->rseed);
	}

	// scan over target and process aggregates per group by key
	scan(self, &select->from,
	     NULL,
	     NULL,
	     select->expr_where,
	     on_match_aggs,
	     plan);

	// select aggr [without group by]
	//
	// force create empty record by processing one NULL value
	if (! select->group_by_has)
		on_match_aggs_empty(self, select);

	// free seed values
	node = select->expr_aggs.list;
	for (; node; node = node->next)
	{
		auto agg = ast_agg_of(node->ast);
		if (! agg->expr_seed)
			continue;
		agg->rseed = emit_free(self, agg->rseed);
	}
}

static void
cmd_pipe(Compiler* self, Plan* plan, Command* ref)
{
	unused(self);
	unused(ref);
	assert(plan->r != -1);
	auto select = plan->select;
	from_first(&select->from_group)->r = plan->r;
}

static void
cmd_sort(Compiler* self, Plan* plan, Command* ref)
{
	unused(ref);
	assert(plan->r != -1);
	op1(self, CSET_SORT, plan->r);
}

static void
cmd_union(Compiler* self, Plan* plan, Command* ref)
{
	unused(ref);
	auto select = plan->select;
	assert(plan->r != -1);

	UnionType type = UNION_ORDERED;
	if (select->distinct)
		type = UNION_ORDERED_DISTINCT;

	// CUNION
	auto runion = op2pin(self, CUNION, TYPE_STORE, type);

	// limit
	if (select->expr_limit)
	{
		/// CUNION_LIMIT
		auto r = emit_expr(self, &select->from, select->expr_limit);
		op2(self, CUNION_LIMIT, runion, r);
		runpin(self, r);
	}

	// offset
	if (select->expr_offset)
	{
		/// CUNION_OFFSET
		auto r = emit_expr(self, &select->from, select->expr_offset);
		op2(self, CUNION_OFFSET, runion, r);
		runpin(self, r);
	}

	// CUNION_ADD
	op2(self, CUNION_ADD, runion, plan->r);
	runpin(self, plan->r);

	plan->r = runion;
}

static void
cmd_set_agg_merge(Compiler* self, Plan* plan, Command* ref)
{
	unused(ref);
	auto select = plan->select;
	assert(plan->r != -1);

	// CSET_AGG_MERGE
	op2(self, CSET_AGG_MERGE, plan->r, select->aggs);
}

static void
cmd_recv(Compiler* self, Plan* plan, Command* ref)
{
	unused(ref);
	auto select = plan->select;

	// set union type
	UnionType type = UNION_ALL;
	if (ref->id == COMMAND_RECV_ORDERED)
	{
		if (select->distinct)
			type = UNION_ORDERED_DISTINCT;
		else
			type = UNION_ORDERED;
	}

	// CUNION
	auto runion = op2pin(self, CUNION, TYPE_STORE, type);

	// limit
	if (select->expr_limit)
	{
		/// CUNION_LIMIT
		auto r = emit_expr(self, &select->from, select->expr_limit);
		op2(self, CUNION_LIMIT, runion, r);
		runpin(self, r);
	}

	// offset
	if (select->expr_offset)
	{
		/// CUNION_OFFSET
		auto r = emit_expr(self, &select->from, select->expr_offset);
		op2(self, CUNION_OFFSET, runion, r);
		runpin(self, r);
	}

	// CRECV
	op2(self, CRECV, runion, self->current->rdispatch);

	plan->r = runion;
}

static void
cmd_recv_aggs(Compiler* self, Plan* plan, Command* ref)
{
	unused(ref);
	auto select = plan->select;

	// CRECV_AGGS
	plan->r = op3pin(self, CRECV_AGGS, TYPE_STORE,
	                 self->current->rdispatch,
	                 select->aggs);
}

typedef void (*PlanFunction)(Compiler*, Plan*, Command*);

static PlanFunction cmds[] =
{
	[COMMAND_EXPR]          = cmd_expr,
	[COMMAND_EXPR_SET]      = cmd_expr_set,
	[COMMAND_SCAN]          = cmd_scan,
	[COMMAND_SCAN_ORDERED]  = cmd_scan,
	[COMMAND_SCAN_AGGS]     = cmd_scan_aggs,
	[COMMAND_PIPE]          = cmd_pipe,
	[COMMAND_SORT]          = cmd_sort,
	[COMMAND_UNION]         = cmd_union,
	[COMMAND_SET_AGG_MERGE] = cmd_set_agg_merge,
	[COMMAND_RECV]          = cmd_recv,
	[COMMAND_RECV_ORDERED]  = cmd_recv,
	[COMMAND_RECV_AGGS]     = cmd_recv_aggs
};

static int
emit_plan(Compiler* self, Plan* plan, bool recv)
{
	auto list = &plan->list;
	if (recv)
		list = &plan->list_recv;
	plan->r = -1;
	list_foreach(list)
	{
		auto cmd = list_at(Command, link);
		cmds[cmd->id](self, plan, cmd);
	}
	return plan->r;
}

int
emit_select(Compiler* self, Ast* ast, bool emit_store)
{
	AstSelect* select = ast_select_of(ast);
	if (! select->plan)
		select->plan = plan_create(ast, emit_store);
	return emit_plan(self, select->plan, false);
}

int
emit_select_recv(Compiler* self, Ast* ast)
{
	AstSelect* select = ast_select_of(ast);
	assert(select->plan);
	return emit_plan(self, select->plan, true);
}
