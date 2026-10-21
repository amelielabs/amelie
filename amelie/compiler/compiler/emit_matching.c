
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
#include <amelie_copy.h>
#include <amelie_plan.h>
#include <amelie_compiler.h>

int
emit_matching(Compiler* self, Ast* ast)
{
	auto matching = ast_matching_of(ast);
	auto target   = from_first(&matching->from);

	// CTABLE_PREPARE
	auto rcursor = op2pin(self, CTABLE_PREPARE, TYPE_CURSOR,
	                      (intptr_t)target->from_table);

	// CMATCHING(rset, cursor, column, top)
	auto rset = op4pin(self, CMATCHING, TYPE_STORE,
	                   rcursor,
	                   (intptr_t)matching->column,
	                   matching->expr_top->integer);

	// CFREE
	emit_free(self, rcursor);
	return rset;
}
