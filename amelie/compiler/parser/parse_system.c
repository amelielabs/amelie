
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

void
parse_system_alter(Stmt* self)
{
	// ALTER SYSTEM SET LIMIT name = value
	// ALTER SYSTEM UNSET LIMIT name
	// ALTER SYSTEM SECRET ROTATE
	auto stmt = ast_system_alter_allocate();
	self->ast = &stmt->ast;

	// SET
	if (stmt_if(self, KSET))
	{
		// LIMIT
		stmt_expect(self, KLIMIT);

		// name
		auto name = stmt_next_shadow(self);
		if (name->id != KNAME)
			stmt_error(self, name, "limit name expected");

		// =
		stmt_expect(self, '=');

		// value
		auto value = stmt_expect(self, KINT);

		// cdc
		if (! str_is_case(&name->string, "cdc", 3))
			stmt_error(self, name, "failed to find the limit");
		if (value->integer < 0)
			stmt_error(self, value, "invalid limit value");

		stmt->cdc_limit = value->integer;
		stmt->type = SYSTEM_ALTER_SET_CDC;
		return;
	}

	// UNSET
	if (stmt_if(self, KUNSET))
	{
		// LIMIT
		stmt_expect(self, KLIMIT);

		// name
		auto name = stmt_next_shadow(self);
		if (name->id != KNAME)
			stmt_error(self, name, "limit name expected");

		// cdc
		if (! str_is_case(&name->string, "cdc", 3))
			stmt_error(self, name, "failed to find the limit");

		stmt->cdc_limit = UINT64_MAX;
		stmt->type = SYSTEM_ALTER_UNSET_CDC;
		return;
	}

	// SECRET
	auto ast = stmt_next_shadow(self);
	if (ast->id != KNAME && !str_is_case(&ast->string, "SECRET", 6))
		stmt_error(self, ast, "SET, UNSET or SECRET expected");

	// ROTATE
	ast = stmt_next_shadow(self);
	if (ast->id != KNAME && !str_is_case(&ast->string, "ROTATE", 6))
		stmt_error(self, ast, "ROTATE expected");

	stmt->type = SYSTEM_ALTER_SECRET_ROTATE;
}
