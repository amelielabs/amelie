
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

static inline void
emit_select_on_match_nested(Compiler* self, void* arg)
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

static inline void
emit_select_on_match(Compiler* self, void* arg)
{
	AstSelect* select = arg;
	int rexpr = emit_select_expr(self, select);
	op1(self, CSEND, rexpr);
	runpin(self, rexpr);
}

static inline void
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

	op1(self, CGROUP_ADD, select->rgroup);
}

static inline void
expr_select_on_match_group(Compiler* self, void* arg)
{
	AstSelect* select = arg;

	// set or send
	ScanFunction on_match = select->on_match;	
	on_match(self, arg);
}

hot int
emit_select(Compiler* self, Ast* ast, bool nested)
{
	AstSelect* select = ast_select_of(ast);

	// SELECT expr[, ...]
	if (select->target == NULL)
	{
		int rexpr = emit_select_expr(self, select);
		if (nested)
			return rexpr;
		op1(self, CSEND, rexpr);
		runpin(self, rexpr);
		// not used
		return -1;
	}

	//
	// SELECT expr, ... [FROM name, [...]]
	// [GROUP BY]
	// [WHERE expr]
	// [ORDER BY]
	// [LIMIT expr] [OFFSET expr]
	//
	int rresult = -1;
	if (select->expr_order_by.count == 0)
	{
		// create data set for nested queries
		if (nested)
		{
			rresult = op1(self, CSET, rpin(self));
			select->rset = rresult;
			select->on_match = emit_select_on_match_nested;
		} else
		{
			select->on_match = emit_select_on_match;
		}
	} else
	{
		// write order by key types
		int  offset = code_data_pos(&self->code_data);
		auto data = &self->code_data.data;
		// [[], ..]
		encode_array(data, select->expr_order_by.count);
		auto node = select->expr_order_by.list;
		while (node)
		{
			auto order = ast_order_of(node->ast);
			// [type, asc]
			encode_array(data, 2);
			encode_integer(data, order->type);
			encode_bool(data, order->asc);
			node = node->next;
		}

		rresult = op2(self, CSET_ORDERED, rpin(self), offset);
		select->rset = rresult;
		select->on_match = emit_select_on_match_nested;
	}

	if (select->target_group == NULL)
	{
		// scan for table/expression and joins
		scan(self,
		     select->target,
		     select->expr_limit,
		     select->expr_offset,
		     select->expr_where,
		     NULL,
		     select->on_match,
		     select);
	} else
	{
		// group by scan
		// aggregation without group by keys

		// create group
		int rgroup;
		rgroup = op2(self, CGROUP, rpin(self), select->expr_group_by.count);
		select->rgroup = rgroup;

		// add aggregates to the group
		auto node = select->target->aggs.list;
		while (node)
		{
			auto aggr = ast_aggr_of(node->ast);
			op3(self, CGROUP_ADD_AGGR, rgroup, aggr->aggregate_id, aggr->order);
			node = node->next;
		}

		// set targets
		auto target_group = select->target_group;
		assert(target_group != NULL);
		target_group->rexpr = rgroup;

		// scan over target and process aggregates per group by key
		scan(self, select->target,
		     NULL,
		     NULL,
		     select->expr_where,
		     NULL,
		     emit_select_on_match_group_target,
		     select);

		// scan over created group

		// redirect each target reference to the group scan target
		target_group_redirect(select->target, target_group);

		scan(self,
		     target_group,
		     select->expr_limit,
		     select->expr_offset,
		     select->expr_having,
		     NULL,
		     expr_select_on_match_group,
		     select);

		target_group_redirect(select->target, NULL);
	}

	// finilize order by
	if (select->expr_order_by.count > 0)
	{
		// sort ordered set
		op1(self, CSET_SORT, rresult);
		if (! nested)
		{
			// send set without creating array
			op1(self, CSET_SEND, rresult);
			runpin(self, rresult);
			rresult = -1;
		}
	}

	return rresult;
}
