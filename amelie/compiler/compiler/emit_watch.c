
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

	// get public.sleep()
	Str schema;
	str_set(&schema, "public", 6);
	Str name;
	str_set(&name, "sleep", 5);
	auto func = function_mgr_find(self->parser.function_mgr, &schema, &name);
	assert(func);

	// call sleep(1000)
	int r = op2(self, CINT, rpin(self, TYPE_INT), 1000);
	op1(self, CPUSH, r);
	runpin(self, r);

	// CALL
	r = op4(self, CCALL, rpin(self, func->type), (intptr_t)func, 1, -1);

	// jmp _start
	op1(self, CJMP, _start);

	// _end
	int _end = op_pos(self);
	op_at(self, true_jmp)->a = _end;

	// nop
	op0(self, CNOP);
	return -1;
}
