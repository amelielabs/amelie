
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
#include <sonata_planner.h>
#include <sonata_compiler.h>

hot void
emit_upsert(Compiler* self, Ast* ast)
{
	auto insert = ast_insert_of(ast);
	auto target = insert->target;
	auto table  = target->table;

	// create returning set
	int rset = -1;
	if (insert->returning)
		rset = op1(self, CSET, rpin(self));

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
		// WHERE expression
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

	// _returning:
	int jmp_returning = -1;

	// [RETURNING]
	if (insert->returning)
	{
		jmp_returning = op_pos(self);

		// expr
		int rexpr = emit_expr(self, target, insert->returning);

		// add to the returning set
		op2(self, CSET_ADD, rset, rexpr);
		runpin(self, rexpr);
	}

	// _start:

	// set jmp_start to _start
	op_at(self, jmp_start)->a = op_pos(self);

	// CUPSERT
	op3(self, CUPSERT, target->id, jmp_where, jmp_returning);
	
	// CCLOSE_CURSOR
	op1(self, CCURSOR_CLOSE, target->id);

	// CRESULT (returning)
	if (insert->returning)
	{
		op1(self, CRESULT, rset);
		runpin(self, rset);
	}
}
