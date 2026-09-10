
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
	// ALTER SYSTEM SET name = value
	// ALTER SYSTEM RESET name
	auto stmt = ast_system_alter_allocate();
	self->ast = &stmt->ast;

	// SET
	if (stmt_if(self, KSET))
	{
		// name = value
		auto name = stmt_next_shadow(self);
		if (name->id != KNAME)
			stmt_error(self, name, "name expected");

		// =
		stmt_expect(self, '=');

		// secret
		if (str_is_case(&name->string, "secret", 6))
		{
			auto value = stmt_expect(self, KSTRING);
			stmt->secret = value->string;
			stmt->type = SYSTEM_ALTER_SET_SECRET;
			return;
		}

		// cdc
		if (str_is_case(&name->string, "cdc", 3))
		{
			// value
			auto value = stmt_expect(self, KINT);
			if (value->integer < 0)
				stmt_error(self, value, "invalid limit value");
			stmt->cdc_limit = value->integer;
			stmt->type = SYSTEM_ALTER_SET_CDC;
			return;
		}

		stmt_error(self, name, "unknown option");
		return;
	}

	// RESET
	if (stmt_if(self, KRESET))
	{
		// name
		auto name = stmt_next_shadow(self);
		if (name->id != KNAME)
			stmt_error(self, name, "name expected");

		// secret
		if (str_is_case(&name->string, "secret", 6))
		{
			// generate new ssecret
			uint8_t* secret = palloc(32);
			random_generate_alnum(&self->parser->local->random, secret, 32);
			str_set_u8(&stmt->secret, secret, 32);
			stmt->type = SYSTEM_ALTER_SET_SECRET;
			return;
		}

		// cdc
		if (str_is_case(&name->string, "cdc", 3))
		{
			stmt->cdc_limit = UINT64_MAX;
			stmt->type = SYSTEM_ALTER_SET_CDC;
			return;
		}

		stmt_error(self, name, "unknown option or unsupported command");
		return;
	}
}
