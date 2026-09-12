
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
parse_backup(Stmt* self)
{
	// BACKUP [ID id]
	auto stmt = ast_backup_allocate();
	self->ast = &stmt->ast;

	uuid_init(&stmt->id);
	auto local = self->parser->local;
	uuid_generate(&stmt->id, &local->random, local->time_ms);

	// set options
	for (;;)
	{
		// name value
		auto name = stmt_next_shadow(self);
		if (name->id != KNAME)
		{
			stmt_push(self, name);
			break;
		}

		// ID string
		if (str_is_case(&name->string, "id", 2))
		{
			auto value = stmt_expect(self, KSTRING);
			Uuid id;
			uuid_init(&id);
			if (uuid_set_nothrow(&id, &value->string) == -1)
				stmt_error(self, value, "failed to parse uuid");
			stmt->id = id;
			continue;
		}

		stmt_error(self, name, "unrecognized option");
	}
}
