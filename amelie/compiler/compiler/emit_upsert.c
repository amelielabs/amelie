
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
#include <amelie_storage>
#include <amelie_repl>
#include <amelie_vm>
#include <amelie_parser.h>
#include <amelie_plan.h>
#include <amelie_compiler.h>

hot int
emit_upsert(Compiler* self, Ast* ast)
{
	auto insert = ast_insert_of(ast);
	auto target = from_first(&insert->from);
	auto table  = target->from_table;

	// set target origin
	target_set_origin(target, self->origin);

	// create returning set
	int rset = -1;
	if (returning_has(&insert->ret))
		rset = op3pin(self, CSET, TYPE_STORE, insert->ret.count, 0);

	// CTABLE_PREPARE
	target->rcursor = op2pin(self, CTABLE_PREPARE, TYPE_CURSOR,
	                         (intptr_t)table);

	// jmp _start
	int jmp_start = op_pos(self);
	op1(self, CJMP, 0 /* _start */);

	// _where:
	int jmp_where = op_pos(self);

	// ON CONFLICT UPDATE or do nothing
	switch (insert->on_conflict) {
	case ON_CONFLICT_UPDATE:
	{
		// WHERE expression
		int jmp_where_jntr = 0;
		if (insert->update_where)
		{
			// expr
			int rexpr;
			rexpr = emit_expr(self, &insert->from, insert->update_where);

			// jntr _start
			jmp_where_jntr = op_pos(self);

			op2(self, CJNTR, 0 /* _start */, rexpr);
			runpin(self, rexpr);
		}

		// update
		emit_update_target(self, &insert->from, insert->update_expr);

		// set jntr _start to _start
		if (insert->update_where)
			op_at(self, jmp_where_jntr)->a = op_pos(self);
		break;
	}
	case ON_CONFLICT_ERROR:
	{
		// find error()
		Str name;
		str_set(&name, "error", 5);
		auto func = function_mgr_find(share()->function_mgr, &name);
		assert(func);

		// call error()
		Str str;
		str_set(&str, "unique key constraint violation", 31);
		int r = emit_string(self, &str, false);
		op1(self, CPUSH, r);
		runpin(self, r);

		// CALL
		r = op4pin(self, CCALL, func->type, (intptr_t)func, 1, -1);
		runpin(self, r);
		break;
	}
	default:
		break;
	}

	// _returning:
	int jmp_returning = -1;

	// [RETURNING]
	if (returning_has(&insert->ret))
	{
		jmp_returning = op_pos(self);

		// push expr and set column type
		for (auto as = insert->ret.list; as; as = as->next)
		{
			auto column = as->r->column;
			// expr
			auto type = emit_push(self, &insert->from, as->l);
			column_set_type(column, type, type_sizeof(type));
		}

		// add to the returning set
		op1(self, CSET_ADD, rset);
	}

	// _start:

	// set jmp_start to _start
	op_at(self, jmp_start)->a = op_pos(self);

	// CUPSERT
	op3(self, CUPSERT, target->rcursor, jmp_where, jmp_returning);
	
	// close and free cursor
	target->rcursor = emit_free(self, target->rcursor);

	// returning set
	return rset;
}
