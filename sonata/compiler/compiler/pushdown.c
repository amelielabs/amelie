
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
#include <sonata_http.h>
#include <sonata_client.h>
#include <sonata_server.h>
#include <sonata_row.h>
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

static inline void
pushdown_group_by(Compiler* self, AstSelect* select)
{
	// SELECT FROM GROUP BY [WHERE] [HAVING] [ORDER BY] [LIMIT/OFFSET]

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

	// CRESULT (return group)
	op1(self, CRESULT, select->rgroup);
	runpin(self, select->rgroup);
	select->rgroup = -1;
}

static inline void
pushdown_order_by(Compiler* self, AstSelect* select)
{
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
	     emit_select_on_match,
	     select);

	// CSET_SORT
	op1(self, CSET_SORT, select->rset);

	// CRESULT (return set)
	op1(self, CRESULT, select->rset);
	runpin(self, select->rset);
}

static inline void
pushdown_limit(Compiler* self, AstSelect* select)
{
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
	     emit_select_on_match,
	     select);

	// CRESULT (return set)
	op1(self, CRESULT, select->rset);
	runpin(self, select->rset);
}

hot void
pushdown(Compiler* self, Ast* ast)
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

	// SELECT FROM GROUP BY [WHERE] [HAVING] [ORDER BY] [LIMIT/OFFSET]
	// SELECT aggregate FROM
	if (select->target_group)
	{
		pushdown_group_by(self, select);
		return;
	}

	// SELECT FROM [WHERE] ORDER BY [LIMIT/OFFSET]
	if (select->expr_order_by.count > 0)
	{
		pushdown_order_by(self, select);
		return;
	}

	// SELECT FROM [WHERE] LIMIT/OFFSET
	// SELECT FROM [WHERE]
	pushdown_limit(self, select);
}

void
pushdown_first(Compiler* self, Ast* ast)
{
	// emit select query as is and pushdown to the first node
	int r = emit_select(self, ast);

	// CRESULT (return set)
	op1(self, CRESULT, r);
	runpin(self, r);
}

static inline int
pushdown_group_by_recv_order_by(Compiler* self, AstSelect* select)
{
	// create ordered data set
	int offset = emit_select_order_by_data(self, select, NULL);
	int rset = op2(self, CSET_ORDERED, rpin(self), offset);
	select->rset = rset;
	select->on_match = emit_select_on_match;

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

static int
pushdown_group_by_recv(Compiler* self, AstSelect* select)
{
	// merge all received groups into one
	//
	// CGROUP_MERGE_RECV
	auto rgroup = op2(self, CGROUP_MERGE_RECV, rpin(self), self->current->order);
	select->rgroup = rgroup;

	auto target_group = select->target_group;
	target_group->rexpr = rgroup;

	// [ORDER BY]
	if (select->expr_order_by.count > 0)
		return pushdown_group_by_recv_order_by(self, select);

	// create set
	int rset = op1(self, CSET, rpin(self));
	select->rset = rset;
	select->on_match = emit_select_on_match;

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

int
pushdown_recv(Compiler* self, Ast* ast)
{
	// all functions below return MERGE/SET on coordinator
	AstSelect* select = ast_select_of(ast);

	// CRECV
	op1(self, CRECV, self->current->order);

	// SELECT FROM GROUP BY [WHERE] [HAVING] [ORDER BY] [LIMIT/OFFSET]
	// SELECT aggregate FROM
	if (select->target_group)
		return pushdown_group_by_recv(self, select);

	// SELECT FROM [WHERE] ORDER BY [LIMIT/OFFSET]
	// SELECT FROM [WHERE] LIMIT/OFFSET
	// SELECT FROM [WHERE]

	// create merge object using received sets and apply
	// distinct/limit/offset
	
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
	int rmerge = op4(self, CMERGE_RECV, rpin(self), rlimit, roffset,
	                 self->current->order);

	if (rlimit != -1)
		runpin(self, rlimit);
	if (roffset != -1)
		runpin(self, roffset);

	return rmerge;
}
