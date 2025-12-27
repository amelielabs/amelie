
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
#include <amelie_engine>
#include <amelie_storage>
#include <amelie_repl>
#include <amelie_vm>
#include <amelie_parser.h>

void
parse_db_create(Stmt* self)
{
	// CREATE DATABASE [IF NOT EXISTS] name
	auto stmt = ast_db_create_allocate();
	self->ast = &stmt->ast;

	// if not exists
	stmt->if_not_exists = parse_if_not_exists(self);

	// create db config
	stmt->config = db_config_allocate();

	// name
	auto name = stmt_expect(self, KNAME);
	db_config_set_name(stmt->config, &name->string);
}

void
parse_db_drop(Stmt* self)
{
	// DROP DATABASE [IF EXISTS] name [CASCADE]
	auto stmt = ast_db_drop_allocate();
	self->ast = &stmt->ast;

	// if exists
	stmt->if_exists = parse_if_exists(self);

	// name
	stmt->name = stmt_expect(self, KNAME);

	// [CASCADE]
	if (stmt_if(self, KCASCADE))
		stmt->cascade = true;
}

void
parse_db_alter(Stmt* self)
{
	// ALTER DATABASE [IF EXISTS] name RENAME name
	auto stmt = ast_db_alter_allocate();
	self->ast = &stmt->ast;

	// if exists
	stmt->if_exists = parse_if_exists(self);

	// name
	stmt->name = stmt_expect(self, KNAME);

	// RENAME
	stmt_expect(self, KRENAME);

	// TO
	stmt_expect(self, KTO);

	// name
	stmt->name_new = stmt_expect(self, KNAME);
}
