
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
parse_system_alter(Stmt* self)
{
	// ALTER SYSTEM SECRET ROTATE
	auto stmt = ast_system_alter_allocate();
	self->ast = &stmt->ast;

	// SECRET
	auto ast = stmt_next_shadow(self);
	if (ast->id != KNAME && !str_is_case(&ast->string, "SECRET", 6))
		stmt_error(self, ast, "SECRET expected");

	// ROTATE
	ast = stmt_next_shadow(self);
	if (ast->id != KNAME && !str_is_case(&ast->string, "ROTATE", 6))
		stmt_error(self, ast, "ROTATE expected");

	stmt->type = SYSTEM_ALTER_SECRET_ROTATE;
}
