
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

void
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

	// find sleep()
	Str name;
	str_set(&name, "sleep", 5);
	auto func = function_mgr_find(share()->function_mgr, &name);
	assert(func);

	// call sleep(1000)
	int r = op2pin(self, CINT, TYPE_INT, 1000);
	op1(self, CPUSH, r);
	runpin(self, r);

	// CALL
	r = op4pin(self, CCALL, func->type, (intptr_t)func, 1, -1);

	// jmp _start
	op1(self, CJMP, _start);

	// _end
	int _end = op_pos(self);
	op_at(self, true_jmp)->a = _end;

	// nop
	op0(self, CNOP);
}
