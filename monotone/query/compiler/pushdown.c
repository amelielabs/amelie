
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
pushdown_group_by(Compiler* self, AstSelect* select)
{
	(void)self;
	(void)select;
	// todo
	return -1;
}

static inline int
pushdown_order_by(Compiler* self, AstSelect* select)
{
	// shard

	// write order by key types
	bool desc   = false;
	int  offset = emit_select_order_by(self, &select->ast, &desc);

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
		if (select->expr_limit && select->expr_offset)
		{
			limit = ast('+');
			limit->l = select->expr_limit;
			limit->r = select->expr_offset;
		} else
		if (select->expr_limit) {
			limit = select->expr_limit;
		} else {
			limit = select->expr_offset;
		}
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

	// coordinator
	dispatch_copy(self->dispatch, &self->code_stmt, self->current->order);
	compiler_switch(self, &self->code_coordinator);

	// CRECV
	op0(self, CRECV);

	// limit
	int rlimit = -1;
	if (select->expr_limit)
		rlimit = emit_expr(self, select->target, select->expr_limit);

	// offset
	int roffset = -1;
	if (select->expr_offset)
		roffset = emit_expr(self, select->target, select->expr_offset);

	// CMERGE
	int rmerge = op4(self, CMERGE, rpin(self),
	                 self->current->order, rlimit, roffset);

	if (rlimit != -1)
		runpin(self, rlimit);
	if (roffset != -1)
		runpin(self, roffset);

	return rmerge;
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
	} else
	if (select->expr_offset) {
		limit = select->expr_offset;
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

	// coordinator
	dispatch_copy(self->dispatch, &self->code_stmt, self->current->order);
	compiler_switch(self, &self->code_coordinator);

	// CRECV
	op0(self, CRECV);

	// limit
	int rlimit = -1;
	if (select->expr_limit)
		rlimit = emit_expr(self, select->target, select->expr_limit);

	// offset
	int roffset = -1;
	if (select->expr_offset)
		roffset = emit_expr(self, select->target, select->expr_offset);

	// CMERGE
	int rmerge = op4(self, CMERGE, rpin(self),
	                 self->current->order, rlimit, roffset);

	if (rlimit != -1)
		runpin(self, rlimit);
	if (roffset != -1)
		runpin(self, roffset);

	return rmerge;
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

	// coordinator
	dispatch_copy(self->dispatch, &self->code_stmt, self->current->order);
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
	// [ORDER BY]
	// [LIMIT expr] [OFFSET expr]
	//

	// all functions below return CMERGE/CSET on coordinator

	// SELECT FROM GROUP BY [WHERE] [HAVING] [ORDER BY] [LIMIT/OFFSET]
	// SELECT aggregate FROM
	if (select->target_group)
		return pushdown_group_by(self, select);

	// SELECT FROM ORDER BY [WHERE] [LIMIT/OFFSET]
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
