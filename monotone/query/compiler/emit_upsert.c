
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

hot static inline void
emit_upsert_op(Compiler* self, AstInsert* insert,
               int       data,
               int       data_size, int* jmp)
{
	auto target = insert->target;
	if (*jmp == 0)
	{
		// cursor_prepare
		op2(self, CCURSOR_PREPARE, target->id, (intptr_t)target->table);

		// jmp _start
		int jmp_start = op_pos(self);
		op1(self, CJMP, 0 /* _start */);

		// _where:
		*jmp = op_pos(self);

		// ON CONFLICT UPDATE or do nothing
		if (insert->on_conflict == ON_CONFLICT_UPDATE)
		{
			// where expression
			if (insert->update_where)
			{
				// expr
				int rexpr;
				rexpr = emit_expr(self, target, insert->update_where);

				// jntr_pop
				op1(self, CJNTR_POP, rexpr);
				runpin(self, rexpr);
			}

			// update
			emit_update_target(self, target, insert->update_expr);
		}

		// jmp_pop
		op0(self, CJMP_POP);

		// point jmp _start to upsert
		op_at(self, jmp_start)->a = op_pos(self);
	}

	// upsert
	op4(self, CUPSERT, target->id, data, data_size, *jmp);
}

hot static inline void
emit_upsert_row(Compiler* self, AstInsert* insert, AstRow* row, int jmp[])
{
	auto stmt     = self->current;
	auto dispatch = self->dispatch;
	auto target   = insert->target;
	auto def      = table_def(target->table);

	// write row to the code data
	int data_size;
	int data = emit_row(&self->code_data, row, &data_size);

	// validate row and hash keys
	auto hash = row_hash(def, code_data_at(&self->code_data, data), data_size);

	// get route by hash
	auto route = router_get(self->router, hash);

	// get the request
	auto req = dispatch_add(dispatch, stmt->order, route);
	compiler_switch(self, &req->code);

	// generate upsert logic
	emit_upsert_op(self, insert, data, data_size, &jmp[route->order]);
}

hot static inline void
emit_upsert_reference(Compiler* self, AstInsert* insert)
{
	// coordinator
	compiler_switch(self, &self->code_coordinator);

	// foreach row generate upsert
	int jmp = 0;
	for (auto i = insert->rows; i; i = i->next)
	{
		auto row = ast_row_of(i);

		// write row to the code data
		int data_size;
		int data = emit_row(&self->code_data, row, &data_size);

		// generate upsert logic
		emit_upsert_op(self, insert, data, data_size, &jmp);
	}

	// CCLOSE_CURSOR
	op1(self, CCURSOR_CLOSE, insert->target->id);
}

hot void
emit_upsert(Compiler* self, Ast* ast)
{
	auto insert   = ast_insert_of(ast);
	auto stmt     = self->current;
	auto dispatch = self->dispatch;
	auto target   = insert->target;

	// reference table
	if (insert->target->table->config->reference)
	{
		// execute upsert by coordinator directly
		emit_upsert_reference(self, insert);
		return;
	}

	// distributed table
	int jmp[dispatch->set_size];
	memset(jmp, 0, dispatch->set_size * sizeof(int));

	// foreach row generate upsert
	for (auto i = insert->rows; i; i = i->next)
	{
		auto row = ast_row_of(i);
		emit_upsert_row(self, insert, row, jmp);
	}

	// close cursors and send ready
	for (int order = 0; order < self->dispatch->set_size; order++)
	{
		auto req = dispatch_at_stmt(self->dispatch, stmt->order, order);
		if (! req)
			continue;

		// CCLOSE_CURSOR
		code_add(&req->code, CCURSOR_CLOSE, target->id, 0, 0, 0);

		// CREADY
		code_add(&req->code, CREADY, stmt->order, -1, 0, 0);
	}

	// coordinator
	compiler_switch(self, &self->code_coordinator);

	// CRECV
	op0(self, CRECV);
}
