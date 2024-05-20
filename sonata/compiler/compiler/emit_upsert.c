
//
// sonata.
//
// SQL Database for JSON.
//

#include <sonata_runtime.h>
#include <sonata_io.h>
#include <sonata_lib.h>
#include <sonata_data.h>
#include <sonata_config.h>
#include <sonata_auth.h>
#include <sonata_http.h>
#include <sonata_client.h>
#include <sonata_server.h>
#include <sonata_def.h>
#include <sonata_transaction.h>
#include <sonata_index.h>
#include <sonata_storage.h>
#include <sonata_db.h>
#include <sonata_value.h>
#include <sonata_aggr.h>
#include <sonata_executor.h>
#include <sonata_vm.h>
#include <sonata_parser.h>
#include <sonata_semantic.h>
#include <sonata_compiler.h>

hot void
emit_upsert(Compiler* self, Ast* ast)
{
	auto insert = ast_insert_of(ast);
	auto target = insert->target;
	auto table  = target->table;

	// emit rows
	insert->rows_offset = code_data_offset(&self->code_data);

	// []
	encode_array(&self->code_data.data, insert->rows_count);

	auto i = insert->rows;
	for (; i; i = i->next)
	{
		auto row = ast_row_of(i);
		int data_size;
		emit_row(&self->code_data, row, &data_size);
	}

	// generate upsert

	// CCLOSE_PREPARE
	op2(self, CCURSOR_PREPARE, target->id, (intptr_t)table);

	// jmp _start
	int jmp_start = op_pos(self);
	op1(self, CJMP, 0 /* _start */);

	// _where:
	int jmp_where = op_pos(self);

	// ON CONFLICT UPDATE or do nothing
	if (insert->on_conflict == ON_CONFLICT_UPDATE)
	{
		// where expression
		int jmp_where_jntr = 0;
		if (insert->update_where)
		{
			// expr
			int rexpr;
			rexpr = emit_expr(self, target, insert->update_where);

			// jntr _start
			jmp_where_jntr = op_pos(self);

			op2(self, CJNTR, 0 /* _start */, rexpr);
			runpin(self, rexpr);
		}

		// update
		emit_update_target(self, target, insert->update_expr);

		// set jntr _start to _start
		if (insert->update_where)
			op_at(self, jmp_where_jntr)->a = op_pos(self);
	}

	// _start:

	// set jmp_start to _start
	op_at(self, jmp_start)->a = op_pos(self);

	// CUPSERT
	op2(self, CUPSERT, target->id, jmp_where);
	
	// CCLOSE_CURSOR
	op1(self, CCURSOR_CLOSE, target->id);
}
