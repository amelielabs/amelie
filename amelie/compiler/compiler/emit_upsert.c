
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
#include <amelie_heap.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_backup.h>
#include <amelie_repl.h>
#include <amelie_value.h>
#include <amelie_set.h>
#include <amelie_content.h>
#include <amelie_executor.h>
#include <amelie_func.h>
#include <amelie_vm.h>
#include <amelie_parser.h>
#include <amelie_compiler.h>

hot void
emit_upsert(Compiler* self, Ast* ast)
{
	auto insert = ast_insert_of(ast);
	auto target = targets_outer(&insert->targets);
	auto table  = target->from_table;

	// create returning set
	int rset = -1;
	if (returning_has(&insert->ret))
		rset = op3(self, CSET, rpin(self, TYPE_STORE), insert->ret.count, 0);

	// CTABLE_PREPARE
	op2(self, CTABLE_PREPARE, target->id, (intptr_t)table);

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
			rexpr = emit_expr(self, &insert->targets, insert->update_where);

			// jntr _start
			jmp_where_jntr = op_pos(self);

			op2(self, CJNTR, 0 /* _start */, rexpr);
			runpin(self, rexpr);
		}

		// update
		emit_update_target(self, &insert->targets, insert->update_expr);

		// set jntr _start to _start
		if (insert->update_where)
			op_at(self, jmp_where_jntr)->a = op_pos(self);
		break;
	}
	case ON_CONFLICT_ERROR:
	{
		// get public.error()
		Str schema;
		str_set(&schema, "public", 6);
		Str name;
		str_set(&name, "error", 5);
		auto func = function_mgr_find(self->parser.function_mgr, &schema, &name);
		assert(func);

		// call error()
		Str str;
		str_set(&str, "unique key constraint violation", 31);
		int r = emit_string(self, &str, false);
		op1(self, CPUSH, r);
		runpin(self, r);

		// CALL
		r = op4(self, CCALL, rpin(self, func->type), (intptr_t)func, 1, -1);
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
			int rexpr = emit_expr(self, &insert->targets, as->l);
			int rt = rtype(self, rexpr);
			column_set_type(column, rt, type_sizeof(rt));
			op1(self, CPUSH, rexpr);
			runpin(self, rexpr);
		}

		// add to the returning set
		op1(self, CSET_ADD, rset);
	}

	// _start:

	// set jmp_start to _start
	op_at(self, jmp_start)->a = op_pos(self);

	// CUPSERT
	op3(self, CUPSERT, target->id, jmp_where, jmp_returning);
	
	// CTABLE_CLOSE
	op1(self, CTABLE_CLOSE, target->id);

	// CRESULT (returning)
	if (returning_has(&insert->ret))
	{
		op1(self, CRESULT, rset);
		runpin(self, rset);
	}
}
