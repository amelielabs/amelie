
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
#include <amelie_tier>
#include <amelie_storage>
#include <amelie_repl>
#include <amelie_vm>
#include <amelie_parser.h>

void
parse_break(Stmt* self)
{
	// BREAK | CONTINUE
	auto stmt = ast_break_allocate();
	self->ast = &stmt->ast;
	self->is_break = true;
	if (self->id == STMT_BREAK)
		self->block->stmts.count_break++;
	else
		self->block->stmts.count_continue++;
}
