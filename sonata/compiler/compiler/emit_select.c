
//
// sonata.
//
// Real-Time SQL Database.
//

#include <sonata_runtime.h>
#include <sonata_io.h>
#include <sonata_lib.h>
#include <sonata_data.h>
#include <sonata_config.h>
#include <sonata_user.h>
#include <sonata_node.h>
#include <sonata_http.h>
#include <sonata_client.h>
#include <sonata_server.h>
#include <sonata_def.h>
#include <sonata_transaction.h>
#include <sonata_index.h>
#include <sonata_partition.h>
#include <sonata_wal.h>
#include <sonata_db.h>
#include <sonata_value.h>
#include <sonata_aggr.h>
#include <sonata_executor.h>
#include <sonata_vm.h>
#include <sonata_parser.h>
#include <sonata_semantic.h>
#include <sonata_compiler.h>

int
emit_select_expr(Compiler* self, AstSelect* select)
{
	// expr
	if (select->expr_count == 1)
		return emit_expr(self, select->target, select->expr);

	// [expr, ...]
	auto expr = select->expr;
	while (expr)
	{
		// expr
		int rexpr = emit_expr(self, select->target, expr);
		// push
		op1(self, CPUSH, rexpr);
		runpin(self, rexpr);
		expr = expr->next;
	}

	// array
	int rarray = rpin(self);
	op2(self, CARRAY, rarray, select->expr_count);
	return rarray;
}

void
emit_select_on_match(Compiler* self, void* arg)
{
	AstSelect* select = arg;

	// expr
	int rexpr = emit_select_expr(self, select);

	// generate order by key (if any)
	auto node = select->expr_order_by.list;
	while (node)
	{
		auto order = ast_order_of(node->ast);
		int rexpr_order_by;
		rexpr_order_by = emit_expr(self, select->target, order->expr);
		op1(self, CPUSH, rexpr_order_by);
		runpin(self, rexpr_order_by);
		node = node->next;
	}

	// add to the set
	op2(self, CSET_ADD, select->rset, rexpr);
	runpin(self, rexpr);
}

void
emit_select_on_match_group_target(Compiler* self, void* arg)
{
	AstSelect* select = arg;

	// push keys and aggs

	// group by keys
	auto node = select->expr_group_by.list;
	while (node)
	{
		auto group = ast_group_of(node->ast);
		int rexpr = emit_expr(self, select->target, group->expr);
		op1(self, CPUSH, rexpr);
		runpin(self, rexpr);
		node = node->next;
	}

	// aggrs
	node = select->expr_aggs.list;
	while (node)
	{
		auto aggr = ast_aggr_of(node->ast);
		int rexpr = emit_expr(self, select->target, aggr->expr);
		op1(self, CPUSH, rexpr);
		runpin(self, rexpr);
		node = node->next;
	}

	op1(self, CGROUP_WRITE, select->rgroup);
}

void
emit_select_on_match_group(Compiler* self, void* arg)
{
	AstSelect* select = arg;

	// add to the set or send
	ScanFunction on_match = select->on_match;	
	on_match(self, arg);
}

int
emit_select_order_by_data(Compiler* self, AstSelect* select, bool* desc)
{
	// write order by key types
	int  offset = code_data_pos(&self->code_data);
	auto data = &self->code_data.data;

	// [asc/desc, ..]
	encode_array(data);
	auto node = select->expr_order_by.list;
	while (node)
	{
		auto order = ast_order_of(node->ast);
		encode_bool(data, order->asc);
		if (desc && !order->asc)
			*desc = true;
		node = node->next;
	}
	encode_array_end(data);
	return offset;
}

hot int
emit_select_merge(Compiler* self, AstSelect* select)
{
	// sort select set
	op1(self, CSET_SORT, select->rset);

	// distinct
	int rdistinct = op2(self, CBOOL, rpin(self), select->distinct);
	op1(self, CPUSH, rdistinct);
	runpin(self, rdistinct);

	// limit
	int rlimit = -1;
	if (select->expr_limit)
		rlimit = emit_expr(self, select->target, select->expr_limit);

	// offset
	int roffset = -1;
	if (select->expr_offset)
		roffset = emit_expr(self, select->target, select->expr_offset);

	// CMERGE
	int rmerge = op4(self, CMERGE, rpin(self), select->rset,
	                 rlimit, roffset);

	runpin(self, select->rset);
	select->rset = -1;

	if (rlimit != -1)
		runpin(self, rlimit);
	if (roffset != -1)
		runpin(self, roffset);

	return rmerge;
}

hot static void
emit_select_group_by_scan(Compiler* self, AstSelect* select,
                          Ast*      limit,
                          Ast*      offset)
{
	// create group
	int rgroup;
	rgroup = op2(self, CGROUP, rpin(self), select->expr_group_by.count);
	select->rgroup = rgroup;

	// add aggregates to the group
	auto node = select->expr_aggs.list;
	while (node)
	{
		auto aggr = ast_aggr_of(node->ast);
		op3(self, CGROUP_ADD, rgroup, aggr->id, aggr->order);
		node = node->next;
	}

	// select aggr from [without group by]
	if (select->expr_group_by.count == 0)
	{
		// force create empty record per each aggregate
		// by processing NULL value
		node = select->expr_aggs.list;
		while (node)
		{
			int rexpr = op1(self, CNULL, rpin(self));
			op1(self, CPUSH, rexpr);
			runpin(self, rexpr);
			node = node->next;
		}
		op1(self, CGROUP_WRITE, select->rgroup);
	}

	// set target group
	auto target_group = select->target_group;
	assert(target_group != NULL);
	target_group->rexpr = rgroup;

	// scan over target and process aggregates per group by key
	scan(self, select->target,
	     NULL,
	     NULL,
	     select->expr_where,
	     emit_select_on_match_group_target,
	     select);

	// redirect each target reference to the group scan target
	target_group_redirect(select->target, target_group);

	// scan over created group
	//
	// result will be added to set or send directly
	//
	scan(self,
	     target_group,
	     limit,
	     offset,
	     select->expr_having,
	     emit_select_on_match_group,
	     select);

	target_group_redirect(select->target, NULL);

	runpin(self, select->rgroup);
	select->rgroup = -1;
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
		// create data set for nested queries
		select->rset = op1(self, CSET, rpin(self));
		select->on_match = emit_select_on_match;
		rresult = select->rset;

		// generate group by scan using limit/offset
		emit_select_group_by_scan(self, select, select->expr_limit,
		                          select->expr_offset);
	} else
	{
		// write order by key types
		int offset = emit_select_order_by_data(self, select, NULL);
		select->rset = op2(self, CSET_ORDERED, rpin(self), offset);
		select->on_match = emit_select_on_match;

		// generate group by scan
		emit_select_group_by_scan(self, select, NULL, NULL);

		// create merge object and add sorted set, apply limit/offset
		rresult = emit_select_merge(self, select);
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
	int offset = emit_select_order_by_data(self, select, NULL);
	select->rset = op2(self, CSET_ORDERED, rpin(self), offset);
	select->on_match = emit_select_on_match;

	// scan for table/expression and joins
	scan(self,
	     select->target,
	     NULL,
	     NULL,
	     select->expr_where,
	     select->on_match,
	     select);

	// create merge object and add sorted set
	return emit_select_merge(self, select);
}

hot static int
emit_select_scan(Compiler* self, AstSelect* select)
{
	//
	// SELECT expr, ... [FROM name, [...]]
	// [WHERE expr]
	// [LIMIT expr] [OFFSET expr]
	//

	// create data set for nested queries
	int rresult = op1(self, CSET, rpin(self));
	select->rset = rresult;
	select->on_match = emit_select_on_match;

	// scan for table/expression and joins
	scan(self,
	     select->target,
	     select->expr_limit,
	     select->expr_offset,
	     select->expr_where,
	     select->on_match,
	     select);

	return rresult;
}

hot int
emit_select(Compiler* self, Ast* ast)
{
	AstSelect* select = ast_select_of(ast);

	// SELECT expr[, ...]
	if (select->target == NULL)
		return emit_select_expr(self, select);

	//
	// SELECT expr, ... [FROM name, [...]]
	// [GROUP BY]
	// [WHERE expr]
	// [ORDER BY]
	// [LIMIT expr] [OFFSET expr]
	//

	// SELECT FROM GROUP BY [WHERE] [HAVING] [ORDER BY] [LIMIT/OFFSET]
	// SELECT aggregate FROM
	if (select->target_group)
		return emit_select_group_by(self, select);

	// SELECT FROM [WHERE] ORDER BY [LIMIT/OFFSET]
	// SELECT DISTINCT FROM
	if (select->expr_order_by.count > 0)
		return emit_select_order_by(self, select);

	// SELECT FROM [WHERE] LIMIT/OFFSET
	// SELECT FROM [WHERE]
	return emit_select_scan(self, select);
}
