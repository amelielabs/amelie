
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
parse_show(Stmt* self)
{
	// SHOW <section> [name] [ON name] [extended]
	auto stmt = ast_show_allocate();
	self->ast = &stmt->ast;
	self->ret = &stmt->ret;

	// section | option name
	auto name = stmt_next_shadow(self);
	if (name->id != KNAME && name->id != KSTRING)
		stmt_error(self, name, "name expected");
	stmt->section = name->string;

	// [name]
	name = stmt_next(self);
	if (name->id == KNAME || name->id == KSTRING || name->id == KPRIMARY)
		stmt->name = name->string;
	else
		stmt_push(self, name);

	// [ON name]
	if (stmt_if(self, KON))
	{
		name = stmt_next_shadow(self);
		if (name->id != KNAME && name->id != KSTRING)
			stmt_error(self, name, "name expected");
		stmt->on = name->string;
	}

	// [EXTENDED]
	stmt->extended = stmt_if(self, KEXTENDED) != NULL;

	// set returning column
	auto column = column_allocate();
	column_set_name(column, &stmt->section);
	column_set_type(column, TYPE_JSON, -1);
	columns_add(&stmt->ret.columns, column);
}
