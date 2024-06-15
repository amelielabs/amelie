
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
#include <sonata_semantic.h>
#include <sonata_compiler.h>

int
emit_watch(Compiler* self, Ast* ast)
{
	AstWatch* watch = ast_watch_of(ast);

	// WATCH expr

	// _start
	int _start = op_pos(self);

	// expr
	int rexpr = emit_expr(self, NULL, watch->expr);

	// jtr _end
	int true_jmp = op_pos(self);
	op2(self, CJTR, 0 /* _end */, rexpr);
	runpin(self, rexpr);

	// sleep
	op1(self, CSLEEP, 1000);

	// jmp _start
	op1(self, CJMP, _start);

	// _end
	int _end = op_pos(self);
	op_at(self, true_jmp)->a = _end;

	// nop
	op0(self, CNOP);
	return op2(self, CBOOL, rpin(self), true);
}
