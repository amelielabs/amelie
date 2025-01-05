
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
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_value.h>
#include <amelie_set.h>
#include <amelie_content.h>
#include <amelie_executor.h>
#include <amelie_vm.h>
#include <amelie_parser.h>
#include <amelie_planner.h>
#include <amelie_compiler.h>

static inline void
pushdown_group_by(Compiler* self, AstSelect* select)
{
	// SELECT FROM GROUP BY [WHERE] [HAVING] [ORDER BY] [LIMIT/OFFSET]

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

	// select aggr [without group by]
	//
	// force create empty record by processing one NULL value
	if (! select->expr_group_by_has)
		emit_select_on_match_aggregate_empty(self, &select->targets, select);

	// CRESULT (return agg set)
	op1(self, CRESULT, select->rset_agg);
	runpin(self, select->rset_agg);
	select->rset_agg = -1;
}

static inline void
pushdown_order_by(Compiler* self, AstSelect* select)
{
	// write order by key types
	bool desc   = false;
	int  offset = emit_select_order_by_data(self, select, &desc);

	// CSET_ORDERED
	select->rset = op4(self, CSET_ORDERED, rpin(self, TYPE_STORE),
	                   select->ret.count,
	                   select->expr_order_by.count,
	                   offset);

	// push limit as limit = limit + offset if possible
	Ast* limit = NULL;
	if (desc)
	{
		// todo: pushing limit with desc requires index
		//       backwards scan support
	} else
	{
		// todo: push limit only if order by matches chosen index keys
		// on any targets
		//
		// (index must be tree, don't do sort in such case)

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
	     &select->targets,
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
	select->rset = op3(self, CSET, rpin(self, TYPE_STORE), select->ret.count, 0);

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
	     &select->targets,
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
	if (! targets_empty(&select->targets_group))
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
	int rset = op4(self, CSET_ORDERED, rpin(self, TYPE_STORE),
	               select->ret.count,
	               select->expr_order_by.count,
	               offset);
	select->rset = rset;

	// scan over agg set
	//
	// result will be added to set, safe to apply limit/offset
	//
	scan(self,
	     &select->targets_group,
	     NULL,
	     NULL,
	     select->expr_having,
	     emit_select_on_match,
	     select);

	runpin(self, select->rset_agg);
	select->rset_agg = -1;
	auto target_group = targets_outer(&select->targets_group);
	target_group->r = -1;

	// no distinct/limit/offset (return set)
	if (select->distinct    == false &&
	    select->expr_limit  == NULL  &&
	    select->expr_offset == NULL)
	{
		// CSET_SORT
		op1(self, CSET_SORT, select->rset);
		return rset;
	}

	// create union object and apply distinct/limit/offset
	return emit_select_union(self, select);
}

static int
pushdown_group_by_recv(Compiler* self, AstSelect* select)
{
	// merge all received agg sets into one
	//
	// CSET_MERGE
	auto rset_agg = op3(self, CSET_MERGE, rpin(self, TYPE_STORE),
	                    self->current->order,
	                    select->aggs);
	select->rset_agg = rset_agg;

	auto target_group = targets_outer(&select->targets_group);
	target_group->r = rset_agg;

	// [ORDER BY]
	if (select->expr_order_by.count > 0)
		return pushdown_group_by_recv_order_by(self, select);

	// create set
	int rset = op3(self, CSET, rpin(self, TYPE_STORE), select->ret.count, 0);
	select->rset = rset;

	// scan over agg set
	//
	// result will be added to set, safe to apply limit/offset
	//
	scan(self,
	     &select->targets_group,
	     select->expr_limit,
	     select->expr_offset,
	     select->expr_having,
	     emit_select_on_match,
	     select);

	runpin(self, select->rset_agg);
	select->rset_agg = -1;
	target_group->r = -1;

	// set freed after scan completed
	return rset;
}

int
pushdown_recv(Compiler* self, Ast* ast)
{
	// all functions below return UNION/SET on coordinator
	AstSelect* select = ast_select_of(ast);

	// CRECV
	op1(self, CRECV, self->current->order);

	// SELECT FROM GROUP BY [WHERE] [HAVING] [ORDER BY] [LIMIT/OFFSET]
	// SELECT aggregate FROM
	if (! targets_empty(&select->targets_group))
		return pushdown_group_by_recv(self, select);

	// SELECT FROM [WHERE] ORDER BY [LIMIT/OFFSET]
	// SELECT FROM [WHERE] LIMIT/OFFSET
	// SELECT FROM [WHERE]

	// create union object using received sets and apply
	// distinct/limit/offset
	
	// distinct
	int rdistinct = op2(self, CBOOL, rpin(self, TYPE_BOOL), select->distinct);
	op1(self, CPUSH, rdistinct);
	runpin(self, rdistinct);

	// limit
	int rlimit = -1;
	if (select->expr_limit)
	{
		rlimit = emit_expr(self, &select->targets, select->expr_limit);
		if (rtype(self, rlimit) != TYPE_INT)
			error("LIMIT: integer type expected");
	}

	// offset
	int roffset = -1;
	if (select->expr_offset)
	{
		roffset = emit_expr(self, &select->targets, select->expr_offset);
		if (rtype(self, roffset) != TYPE_INT)
			error("OFFSET: integer type expected");
	}

	// CUNION_RECV
	int runion = op4(self, CUNION_RECV, rpin(self, TYPE_STORE), rlimit, roffset,
	                 self->current->order);

	if (rlimit != -1)
		runpin(self, rlimit);
	if (roffset != -1)
		runpin(self, roffset);

	return runion;
}

int
pushdown_recv_returning(Compiler* self, bool returning)
{
	// CRECV
	op1(self, CRECV, self->current->order);
	if (! returning)
		return -1;

	// create union object using received sets

	// distinct
	int rdistinct = op2(self, CBOOL, rpin(self, TYPE_BOOL), false);
	op1(self, CPUSH, rdistinct);
	runpin(self, rdistinct);

	// CUNION_RECV
	int runion = op4(self, CUNION_RECV, rpin(self, TYPE_STORE), -1, -1,
	                 self->current->order);
	return runion;
}
