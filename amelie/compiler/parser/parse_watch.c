
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

void
parse_watch(Stmt* self)
{
	// WATCH expr
	auto stmt = ast_watch_allocate();
	self->ast = &stmt->ast;
	stmt->expr = parse_expr(self, NULL);
}
