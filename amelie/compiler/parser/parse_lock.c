
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
parse_lock_create(Stmt* self)
{
	// CREATE LOCK [IF NOT EXISTS] name ON name (lock)
	auto stmt = ast_lock_create_allocate();
	self->ast = &stmt->ast;

	// if not exists
	stmt->if_not_exists = parse_if_not_exists(self);

	// name
	auto name = stmt_expect(self, KNAME);
	stmt->name = name->string;

	// ON
	stmt_expect(self, KON);

	// name
	name = stmt_expect(self, KNAME);
	stmt->name_rel = name->string;

	// (lock)
	stmt_expect(self, '(');
	name = stmt_expect(self, KNAME);
	stmt->name_lock = name->string;
	stmt_expect(self, ')');
}

void
parse_lock_drop(Stmt* self)
{
	// DROP LOCK [IF EXISTS] id
	auto stmt = ast_lock_drop_allocate();
	self->ast = &stmt->ast;

	// if exists
	stmt->if_exists = parse_if_exists(self);

	// name
	auto name = stmt_expect(self, KNAME);
	stmt->name = name->string;
}
