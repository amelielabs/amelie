
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
#include <amelie_db>
#include <amelie_repl>
#include <amelie_vm>
#include <amelie_parser.h>
#include <amelie_copy.h>
#include <amelie_plan.h>
#include <amelie_compiler.h>

hot void
emit_if(Compiler* self, Stmt* stmt)
{
	auto ifa = ast_if_of(stmt->ast);

	// foreach cond
	//   stmt
	//   jntr _next
	//   block
	//   jmp _stop
	//
	// else_block
	// _stop
	//

	// save jntrs positions
	int  jntr_stop_count = 0;
	int* jntr_stop = palloc(sizeof(int) * ifa->conds.count);

	auto ref = ifa->conds.list;
	for (; ref; ref = ref->next)
	{
		// IF expr
		auto cond = ast_if_cond_of(ref->ast);
		auto r = emit_expr(self, &ifa->from, cond->expr);

		// jntr _next
		int _next_jntr = op_pos(self);
		op2(self, CJNTR, 0 /* _next */, r);
		runpin(self, r);

		// THEN block

		// block
		emit_block(self, cond->block);

		// do not generate jmp for the last if block
		if (ref->next || ifa->cond_else)
		{
			// jmp _stop (save jmp position)
			jntr_stop[cond->order] = op_pos(self);
			jntr_stop_count++;
			op1(self, CJMP, 0 /* _stop */);
		}

		// _next
		op_set_jmp(self, _next_jntr, op_pos(self));
		op_at(self, _next_jntr)->a = op_pos(self);
	}

	// else block
	if (ifa->cond_else)
		emit_block(self, ifa->cond_else);

	// _stop (resolve jmps to _stop)
	auto _stop = op_pos(self);
	for (auto i = 0; i < jntr_stop_count; i++)
		op_set_jmp(self, jntr_stop[i], _stop);

	// mark last sending operation in the main block
	emit_close(self, stmt);
}

static void
emit_for_on_match(Scan* self)
{
	AstFor* fora = self->on_match_arg;
	fora->breaks    = &self->breaks;
	fora->continues = &self->continues;
	emit_block(self->compiler, fora->block);
}

hot void
emit_for(Compiler* self, Stmt* stmt)
{
	auto fora = ast_for_of(stmt->ast);

	// scan over from
	scan(self,
	     &fora->from,
	     NULL,
	     NULL,
	     NULL,
	     emit_for_on_match,
	     fora);

	// mark last sending operation in the main block
	emit_close(self, stmt);
}

hot void
emit_while(Compiler* self, Stmt* stmt)
{
	auto whilea = ast_while_of(stmt->ast);

	// _start:
	//   expr
	//   jntr _stop
	//   block
	//   quota
	//   jmp _start
	//
	// _stop:
	//

	// _start
	auto _start = op_pos(self);

	// expr
	auto r = emit_expr(self, &whilea->from, whilea->expr);

	// jntr _stop
	auto _stop_jmp = op_pos(self);
	op2(self, CJNTR, 0, /* _stop */ r);
	runpin(self, r);

	// block
	emit_block(self, whilea->block);

	// compute limit check
	op0(self, CQUOTA);

	// jmp _start
	op1(self, CJMP, _start);

	// _stop
	auto _stop = op_pos(self);
	op_set_jmp(self, _stop_jmp, _stop);

	// resolve nested breaks/continues
	op_set_jmp_list(self, &whilea->breaks, _stop);
	op_set_jmp_list(self, &whilea->continues, _start);

	// mark last sending operation in the main block
	emit_close(self, stmt);
}

hot void
emit_break(Compiler* self, Stmt* stmt)
{
	// find parent loop stmt
	Buf*  breaks    = NULL;
	Buf*  continues = NULL;
	Stmt* parent    = NULL;

	auto  block = stmt->block;
	while (block && block->parent_stmt)
	{
		auto ref = block->parent_stmt;
		if (ref->id == STMT_WHILE)
		{
			auto whilea = ast_while_of(ref->ast);
			breaks    = &whilea->breaks;
			continues = &whilea->continues;
			parent    = ref;
			break;
		}

		if (ref->id == STMT_FOR)
		{
			auto fora = ast_for_of(ref->ast);
			breaks    = fora->breaks;
			continues = fora->continues;
			parent    = ref;
			break;
		}

		block = block->parent;
	}
	if (! parent)
		stmt_error(stmt, stmt->ast, "used out of loop");

	// emit jmp, it will be resolved by the loop end
	switch (stmt->id) {
	case STMT_BREAK:
		// jmp _stop
		op_pos_add(self, breaks);
		op1(self, CJMP, 0 /* _stop */);
		break;
	case STMT_CONTINUE:
		// jmp _start
		op_pos_add(self, continues);
		op1(self, CJMP, 0 /* _start */);
		break;
	default:
		abort();
		break;
	}
}

hot void
emit_return(Compiler* self, Stmt* stmt)
{
	// return from main
	auto udf = stmt->block->ns->udf;
	if (! udf)
	{
		auto r = -1;
		if (stmt->ret && stmt->ret->columns.count > 0)
			r = stmt->r;
		op1(self, CRET, r);
		return;
	}

	// return from function

	// return;
	if (stmt->id == STMT_RETURN && !stmt->ast)
	{
		op1(self, CRET, -1);
		return;
	}

	// return var | stmt;
	int      r;
	int      type;
	Columns* columns;
	if (stmt->id == STMT_RETURN)
	{
		auto var = stmt->ast->var;
		r = op3pin(self, CVAR, var->type, var->order, var->is_arg);
		type = rtype(self, r);
		columns = &var->columns;
	} else
	{
		if (stmt->ret && stmt->ret->columns.count > 0)
			r = stmt->r;
		else
			r = -1;
		columns = &stmt->ret->columns;
	}
	if (r != -1)
		type = rtype(self, r);
	else
		type = TYPE_NULL;

	// validate return type
	if (type != TYPE_NULL)
	{
		if (type == TYPE_STORE)
		{
			if (udf->type == TYPE_STORE)
			{
				if (! columns_compare(&udf->returning, columns))
					stmt_error(stmt, stmt->ast,
					           "RETURN columns mismatch function returning columns");
			} else
			{
				if (columns->count != 1)
					stmt_error(stmt, stmt->ast, "RETURN expect one column");

				// get first column from the the store
				auto first = columns_first(columns);
				type = first->type;
				auto rfirst = op2pin(self, CFIRST, type, r);
				runpin(self, r);
				r = rfirst;
			}
		}
		if (udf->type != type)
			stmt_error(stmt, stmt->ast, "RETURN type '{s}' mismatch function type '{s}'",
			           type_of(type),
			           type_of(udf->type));
	}

	// CRET
	op1(self, CRET, r);
}
