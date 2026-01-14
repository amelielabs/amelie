
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

void
parse_repl_start(Stmt* self)
{
	// START REPL
	auto stmt = ast_repl_ctl_allocate(true);
	self->ast = &stmt->ast;
}

void
parse_repl_stop(Stmt* self)
{
	// STOP REPL
	auto stmt = ast_repl_ctl_allocate(false);
	self->ast = &stmt->ast;
}

void
parse_repl_subscribe(Stmt* self)
{
	// SUBSCRIBE id
	auto stmt = ast_repl_subscribe_allocate();
	self->ast = &stmt->ast;
	stmt->id  = stmt_expect(self, KSTRING);
}

void
parse_repl_unsubscribe(Stmt* self)
{
	// UNSUBSCRIBE
	auto stmt = ast_repl_subscribe_allocate();
	self->ast = &stmt->ast;
	stmt->id  = NULL;
}
