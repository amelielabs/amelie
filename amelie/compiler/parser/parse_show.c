
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
parse_show(Stmt* self)
{
	// SHOW <SECTION> [name] [IN|FROM db] [extended]
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
	if (name->id == KNAME || name->id == KSTRING) {
		stmt->name = name->string;
	} else
	if (name->id == KNAME_COMPOUND)
	{
		stmt->name = name->string;
		str_split(&stmt->name, &stmt->db, '.');
		str_advance(&stmt->name, str_size(&stmt->db) + 1);
	} else {
		stmt_push(self, name);
	}

	// [IN | FROM db]
	if (stmt_if(self, KIN) || stmt_if(self, KFROM))
	{
		auto db = stmt_expect(self, KNAME);
		stmt->db = db->string;
	}

	// [EXTENDED]
	stmt->extended = stmt_if(self, KEXTENDED) != NULL;

	// set returning column
	auto column = column_allocate();
	column_set_name(column, &stmt->section);
	column_set_type(column, TYPE_JSON, -1);
	columns_add(&stmt->ret.columns, column);
}
