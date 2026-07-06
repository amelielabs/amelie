
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
#include <amelie_plan.h>
#include <amelie_compiler.h>

int
emit_matching(Compiler* self, Ast* ast)
{
	auto matching = ast_matching_of(ast);
	auto target   = from_first(&matching->from);

	// set target origin
	target_set_origin(target, self->origin);

	// CTABLE_PREPARE
	auto rcursor = op3pin(self, CTABLE_PREPARE, TYPE_CURSOR,
	                      (intptr_t)target->from_table,
	                      (intptr_t)target->from_snapshot);

	// CMATCHING(rset, cursor, column, top)
	auto rset = op4pin(self, CMATCHING, TYPE_STORE,
	                   rcursor,
	                   (intptr_t)matching->column,
	                   matching->expr_top->integer);

	// CFREE
	emit_free(self, rcursor);
	return rset;
}

int
emit_matching_recv(Compiler* self, Ast* ast)
{
	// CRECV_MATCHING
	unused(ast);

	// [result, rdispatch]
	return op2pin(self, CRECV_MATCHING, TYPE_STORE,
	              self->current->rdispatch);
}
