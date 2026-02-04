
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
parse_part_alter(Stmt* self)
{
	// ALTER PARTITION id ON table REFRESH
	// ALTER PARTITION id ON table MOVE TO storage
	auto stmt = ast_part_alter_allocate();
	self->ast = &stmt->ast;

	// id
	auto id = stmt_expect(self, KINT);
	stmt->id = id->integer;

	// ON
	stmt_expect(self, KON);

	// table
	stmt->table = stmt_expect(self, KNAME);

	// command
	auto cmd = stmt_next_shadow(self);
	if  (cmd->id != KNAME)
		stmt_error(self, cmd, "command expected");

	// REFRESH | MOVE
	if (str_is_case(&cmd->string, "refresh", 7))
	{
		stmt->type = PARTITION_ALTER_REFRESH;
	} else
	if (str_is_case(&cmd->string, "move", 4))
	{
		stmt->type = PARTITION_ALTER_MOVE;

		// TO
		stmt_expect(self, KTO);

		// name
		auto storage = stmt_expect(self, KNAME);
		stmt->storage = storage->string;
	} else {
		stmt_error(self, cmd, "unknown command");
	}
}
