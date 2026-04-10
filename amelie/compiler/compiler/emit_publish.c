
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
#include <amelie_sync>
#include <amelie_vm>
#include <amelie_parser.h>
#include <amelie_plan.h>
#include <amelie_compiler.h>

void
emit_publish(Compiler* self, Ast* ast)
{
	auto publish = ast_publish_of(ast);

	// expr
	auto rexpr = -1;
	if (publish->expr)
		rexpr = emit_expr(self, NULL, publish->expr);

	// CPUBLISH
	op2(self, CPUBLISH, (intptr_t)publish->channel, rexpr);
}
