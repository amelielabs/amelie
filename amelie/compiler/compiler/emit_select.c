
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
#include <amelie_value.h>
#include <amelie_store.h>
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
#include <amelie_executor.h>
#include <amelie_vm.h>
#include <amelie_parser.h>
#include <amelie_planner.h>
#include <amelie_compiler.h>

int
emit_select_expr(Compiler* self, AstSelect* select)
{
	// push expr and prepare returning columns
	for (auto as = select->ret.list; as; as = as->next)
	{
		// expr
		int rexpr = emit_expr(self, select->target, as->l);
		int rt = rtype(self, rexpr);
		op1(self, CPUSH, rexpr);
		runpin(self, rexpr);

		auto column = column_allocate();
		column_set_name(column, &as->r->string);
		column_set_type(column, rt);
		columns_add(&select->ret.columns, column);
	}
	return select->rset;
}

void
emit_select_on_match(Compiler* self, void* arg)
{
	AstSelect* select = arg;

	// push expressions
	emit_select_expr(self, select);

	// push order by key (if any)
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

	// add to the returning set
	op1(self, CSET_ADD, select->rset);
}

void
emit_select_on_match_group_target(Compiler* self, void* arg)
{
	AstSelect* select = arg;

	// create a list of aggs based on type
	int  aggs_offset = code_data_pos(&self->code_data);
	int* aggs = buf_claim(&self->code_data.data, sizeof(int) * select->expr_aggs.count);
	select->aggs = aggs_offset;

	// push aggs
	auto node = select->expr_aggs.list;
	while (node)
	{
		auto agg = ast_agg_of(node->ast);

		// expr
		auto rexpr = emit_expr(self, select->target, agg->expr);
		auto rt = rtype(self, rexpr);
		op1(self, CPUSH, rexpr);
		runpin(self, rexpr);

		switch (agg->function->id) {
		case KCOUNT:
			agg->id = AGG_INT_COUNT;
			break;
		case KMIN:
			if (rt == VALUE_INT || rt == VALUE_NULL)
				agg->id = AGG_INT_MIN;
			else
			if (rt == VALUE_DOUBLE)
				agg->id = AGG_DOUBLE_MIN;
			else
				error("min(): int or double expected");
			break;
		case KMAX:
			if (rt == VALUE_INT || rt == VALUE_NULL)
				agg->id = AGG_INT_MAX;
			else
			if (rt == VALUE_DOUBLE)
				agg->id = AGG_DOUBLE_MAX;
			else
				error("max(): int or double expected");
			break;
		case KSUM:
			if (rt == VALUE_INT || rt == VALUE_NULL)
				agg->id = AGG_INT_SUM;
			else
			if (rt == VALUE_DOUBLE)
				agg->id = AGG_DOUBLE_SUM;
			else
				error("sum(): int or double expected");
			break;
		case KAVG:
			if (rt == VALUE_INT || rt == VALUE_NULL)
				agg->id = AGG_INT_AVG;
			else
			if (rt == VALUE_DOUBLE)
				agg->id = AGG_DOUBLE_AVG;
			else
				error("avg(): int or double expected");
			break;
		}
		aggs[agg->order] = agg->id;

		// add group target column
		auto column = column_allocate();
		auto name_sz = palloc(8);
		snprintf(name_sz, 8, "agg%d", agg->order + 1);
		Str name;
		str_set_cstr(&name, name_sz);
		column_set_name(column, &name);
		column_set_type(column, rt);
		columns_add(&select->columns_group, column);

		node = node->next;
	}

	// push group by keys
	node = select->expr_group_by.list;
	while (node)
	{
		auto group = ast_group_of(node->ast);

		// expr
		auto rexpr = emit_expr(self, select->target, group->expr);
		auto rt = rtype(self, rexpr);
		op1(self, CPUSH, rexpr);
		runpin(self, rexpr);

		// add group target key column
		auto column = column_allocate();

		// resolve column
		if (group->expr->id == KNAME)
		{
			// find column in the target
			auto target = select->target;
			auto name = &group->expr->string;
			bool conflict = false;
			auto ref  = columns_find_noconflict(target->from_columns, name, &conflict);
			if (! ref)
			{
				if (conflict)
					error("<%.*s.%.*s> column name is ambiguous",
					      str_size(&target->name), str_of(&target->name),
					      str_size(name), str_of(name));
				else
					error("<%.*s.%.*s> column not found",
					      str_size(&target->name), str_of(&target->name),
					      str_size(name), str_of(name));
			}

			// copy column name and type
			column_set_name(column, &ref->name);
			column_set_type(column, ref->type);
		} else
		{
			// generate name
			auto name_sz = palloc(8);
			snprintf(name_sz, 8, "_key%d", group->order + 1);
			Str name;
			str_set_cstr(&name, name_sz);
			column_set_name(column, &name);
			column_set_type(column, rt);
		}
		columns_add(&select->columns_group, column);

		node = node->next;
	}

	// CSET_AGG
	op2(self, CSET_AGG, select->rset_agg, aggs_offset);
}

void
emit_select_on_match_group_target_empty(Compiler* self, AstSelect* select)
{
	// process NULL values for the aggregate

	// push aggs
	auto node = select->expr_aggs.list;
	while (node)
	{
		auto rexpr = op1(self, CNULL, rpin(self, VALUE_NULL));
		op1(self, CPUSH, rexpr);
		runpin(self, rexpr);
		node = node->next;
	}

	// push group by keys
	node = select->expr_group_by.list;
	while (node)
	{
		auto group = ast_group_of(node->ast);
		auto rexpr = emit_expr(self, select->target, group->expr);
		op1(self, CPUSH, rexpr);
		runpin(self, rexpr);
		node = node->next;
	}

	// CSET_AGG
	op2(self, CSET_AGG, select->rset_agg, select->aggs);
}

void
emit_select_on_match_group(Compiler* self, void* arg)
{
	AstSelect* select = arg;

	// add to the set
	ScanFunction on_match = select->on_match;	
	on_match(self, arg);
}

int
emit_select_order_by_data(Compiler* self, AstSelect* select, bool* desc)
{
	// write order by asc/desc flags
	int  order_offset = code_data_pos(&self->code_data);
	int* order = buf_claim(&self->code_data.data, sizeof(int) * select->expr_order_by.count);
	auto node = select->expr_order_by.list;
	while (node)
	{
		auto ref = ast_order_of(node->ast);
		order[ref->order] = ref->asc;
		if (desc && !ref->asc)
			*desc = true;
		node = node->next;
	}
	return order_offset;
}

hot int
emit_select_merge(Compiler* self, AstSelect* select)
{
	// sort select set
	op1(self, CSET_SORT, select->rset);

	// distinct
	int rdistinct = op2(self, CBOOL, rpin(self, VALUE_BOOL), select->distinct);
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
	int rmerge = op4(self, CMERGE, rpin(self, VALUE_MERGE), select->rset,
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
	// create agg set
	int rset;
	rset = op3(self, CSET, rpin(self, VALUE_SET),
	           select->expr_aggs.count,
	           select->expr_group_by.count);
	select->rset_agg = rset;

	// scan over target and process aggregates per group by key
	scan(self, select->target,
	     NULL,
	     NULL,
	     select->expr_where,
	     emit_select_on_match_group_target,
	     select);

	// select aggr [without group by]
	//
	// force create empty record by processing one NULL value
	if (! select->expr_group_by_has)
		emit_select_on_match_group_target_empty(self, select);

	// scan over created group
	//
	// result will be added to set or send directly
	//
	select->target_group->r = rset;
	scan(self,
	     select->target_group,
	     limit,
	     offset,
	     select->expr_having,
	     emit_select_on_match_group,
	     select);

	runpin(self, select->rset_agg);
	select->rset_agg = -1;
	select->target_group->r = -1;
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
		select->rset = op3(self, CSET, rpin(self, VALUE_SET), select->ret.count, 0);
		select->on_match = emit_select_on_match;
		rresult = select->rset;

		// generate group by scan using limit/offset
		emit_select_group_by_scan(self, select, select->expr_limit,
		                          select->expr_offset);
	} else
	{
		// write order by key types
		int offset = emit_select_order_by_data(self, select, NULL);
		select->rset = op4(self, CSET_ORDERED, rpin(self, VALUE_SET),
		                   select->ret.count,
		                   select->expr_order_by.count,
		                   offset);
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

	select->rset = op4(self, CSET_ORDERED, rpin(self, VALUE_SET),
	                   select->ret.count,
	                   select->expr_order_by.count,
	                   offset);
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

	// create result set
	int rresult = op3(self, CSET, rpin(self, VALUE_SET), select->ret.count, 0);
	select->rset = rresult;

	// create data set for nested queries
	select->on_match = emit_select_on_match;

	// scan for table/expression and joins
	scan(self,
	     select->target,
	     select->expr_limit,
	     select->expr_offset,
	     select->expr_where,
	     select->on_match,
	     select);

	return select->rset;
}

hot int
emit_select(Compiler* self, Ast* ast)
{
	AstSelect* select = ast_select_of(ast);

	// SELECT expr[, ...]
	if (select->target == NULL)
	{
		// create result set
		int rresult = op3(self, CSET, rpin(self, VALUE_SET), select->ret.count, 0);
		select->rset = rresult;

		// push expressions
		emit_select_expr(self, select);

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
