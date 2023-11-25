
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
#include <monotone_def.h>
#include <monotone_transaction.h>
#include <monotone_snapshot.h>
#include <monotone_storage.h>
#include <monotone_wal.h>
#include <monotone_db.h>
#include <monotone_value.h>
#include <monotone_aggr.h>
#include <monotone_request.h>
#include <monotone_vm.h>
#include <monotone_parser.h>
#include <monotone_semantic.h>
#include <monotone_compiler.h>

static inline int
pushdown_group_by_order_by(Compiler* self, AstSelect* select)
{
	// create ordered data set
	int offset = emit_select_order_by_data(self, select, NULL);
	int rset = op2(self, CSET_ORDERED, rpin(self), offset);
	select->rset = rset;
	select->on_match = emit_select_on_match_set;

	// redirect each target reference to the group scan target
	auto target_group = select->target_group;
	target_group_redirect(select->target, target_group);

	// scan over created group
	scan(self,
	     target_group,
	     NULL,
	     NULL,
	     select->expr_having,
	     emit_select_on_match_group,
	     select);

	target_group_redirect(select->target, NULL);

	runpin(self, select->rgroup);
	select->rgroup = -1;

	// no distinct/limit/offset (return set)
	if (select->distinct    == false &&
	    select->expr_limit  == NULL &&
	    select->expr_offset == NULL)
	{
		// CSET_SORT
		op1(self, CSET_SORT, select->rset);
		return rset;
	}

	// create merge object and apply distinct/limit/offset
	return emit_select_merge(self, select);
}

static inline int
pushdown_group_by(Compiler* self, AstSelect* select, bool nested)
{
	// SELECT FROM GROUP BY [WHERE] [HAVING] [ORDER BY] [LIMIT/OFFSET]
	auto stmt = self->current;

	// shard

	// create group
	int rgroup;
	rgroup = op2(self, CGROUP, rpin(self), select->expr_group_by.count);
	select->rgroup = rgroup;

	// add aggregates to the group
	auto node = select->expr_aggs.list;
	while (node)
	{
		auto aggr = ast_aggr_of(node->ast);
		op3(self, CGROUP_ADD_AGGR, rgroup, aggr->id, aggr->order);
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
		op1(self, CGROUP_ADD, select->rgroup);
	}

	// set target
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

	// CREADY (return group)
	op2(self, CREADY, stmt->order, select->rgroup);
	runpin(self, select->rgroup);
	select->rgroup = -1;

	// copy statement code to each shard
	compiler_distribute(self);

	// coordinator
	compiler_switch(self, &self->code_coordinator);

	// CRECV
	op0(self, CRECV);

	// merge all received groups into one
	//
	// CGROUP_MERGE_RECV
	rgroup = op2(self, CGROUP_MERGE_RECV, rpin(self), stmt->order);
	select->rgroup = rgroup;
	target_group->rexpr = rgroup;

	// [ORDER BY]
	if (select->expr_order_by.count > 0)
		return pushdown_group_by_order_by(self, select);

	// create data set for nested queries
	int rset = -1;
	if (nested)
	{
		rset = op1(self, CSET, rpin(self));
		select->rset = rset;
		select->on_match = emit_select_on_match_set;
	} else {
		select->on_match = emit_select_on_match;
	}

	// redirect each target reference to the group scan target
	target_group_redirect(select->target, target_group);

	// scan over created group
	//
	// result will be added to set or send directly,
	// safe to apply limit/offset
	//
	scan(self,
	     target_group,
	     select->expr_limit,
	     select->expr_offset,
	     select->expr_having,
	     emit_select_on_match_group,
	     select);

	target_group_redirect(select->target, NULL);

	runpin(self, select->rgroup);
	select->rgroup = -1;

	// group object freed after scan completed
	return rset;
}

hot static inline int
pushdown_merge(Compiler* self, AstSelect* select)
{
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

	// CMERGE_RECV
	int rmerge = op4(self, CMERGE_RECV, rpin(self), self->current->order,
	                 rlimit, roffset);

	if (rlimit != -1)
		runpin(self, rlimit);
	if (roffset != -1)
		runpin(self, roffset);

	return rmerge;
}

static inline int
pushdown_order_by(Compiler* self, AstSelect* select)
{
	// shard

	// write order by key types
	bool desc   = false;
	int  offset = emit_select_order_by_data(self, select, &desc);

	// CSET_ORDERED
	select->rset = op2(self, CSET_ORDERED, rpin(self), offset);

	// push limit as limit = limit + offset if possible
	Ast* limit = NULL;
	if (desc)
	{
		// todo: pushing limit with desc requires index
		//       backwards scan support
	} else
	{
		// push limit only if order by matches primary key
		// (don't do sort in such case)

		/*
		if (select->expr_limit && select->expr_offset)
		{
			limit = ast('+');
			limit->l = select->expr_limit;
			limit->r = select->expr_offset;
		} else
		if (select->expr_limit) {
			limit = select->expr_limit;
		}
		*/
	}

	// scan for table/expression and joins
	scan(self,
	     select->target,
	     limit,
	     NULL,
	     select->expr_where,
	     emit_select_on_match_set,
	     select);

	// CSET_SORT
	op1(self, CSET_SORT, select->rset);

	// CREADY (return set)
	op2(self, CREADY, self->current->order, select->rset);
	runpin(self, select->rset);

	// copy statement code to each shard
	compiler_distribute(self);

	// coordinator
	compiler_switch(self, &self->code_coordinator);

	// CRECV
	op0(self, CRECV);

	// create merge object using received sets and apply
	// distinct/limit/offset
	return pushdown_merge(self, select);
}

static inline int
pushdown_limit(Compiler* self, AstSelect* select)
{
	// shard

	// create result set
	select->rset = op1(self, CSET, rpin(self));

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

	// scan for table/expression and joins
	scan(self,
	     select->target,
	     limit,
	     NULL,
	     select->expr_where,
	     emit_select_on_match_set,
	     select);

	// CREADY (return set)
	op2(self, CREADY, self->current->order, select->rset);
	runpin(self, select->rset);

	// copy statement code to each shard
	compiler_distribute(self);

	// coordinator
	compiler_switch(self, &self->code_coordinator);

	// CRECV
	op0(self, CRECV);

	// create merge object using received sets and apply limit/offset
	return pushdown_merge(self, select);
}

static inline int
pushdown_from(Compiler* self, AstSelect* select)
{
	// shard

	// scan for table/expression and joins
	scan(self,
	     select->target,
	     NULL,
	     NULL,
	     select->expr_where,
	     emit_select_on_match,
	     select);

	// CREADY
	op2(self, CREADY, self->current->order, -1);

	// copy statement code to each shard
	compiler_distribute(self);

	// coordinator
	compiler_switch(self, &self->code_coordinator);

	// CRECV
	op0(self, CRECV);
	return -1;
}

hot int
pushdown(Compiler* self, Ast* ast, bool nested)
{
	AstSelect* select = ast_select_of(ast);

	//
	// SELECT expr, ... [FROM name, [...]]
	// [GROUP BY]
	// [WHERE expr]
	// [HAVING expr]
	// [ORDER BY]
	// [LIMIT expr] [OFFSET expr]
	//

	// all functions below return MERGE/SET on coordinator

	// SELECT FROM GROUP BY [WHERE] [HAVING] [ORDER BY] [LIMIT/OFFSET]
	// SELECT aggregate FROM
	if (select->target_group)
		return pushdown_group_by(self, select, nested);

	// SELECT FROM [WHERE] ORDER BY [LIMIT/OFFSET]
	if (select->expr_order_by.count > 0)
		return pushdown_order_by(self, select);

	// SELECT FROM [WHERE] LIMIT/OFFSET
	if (select->expr_limit || select->expr_offset)
		return pushdown_limit(self, select);

	// SELECT FROM [WHERE]
	if (nested)
		return pushdown_limit(self, select);

	// no return
	return pushdown_from(self, select);
}
