
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
parse_synonym_create(Stmt* self)
{
	// CREATE SYNONYM [user.]name FOR [user.]name
	auto stmt = ast_synonym_create_allocate();
	self->ast = &stmt->ast;

	// if not exists
	stmt->if_not_exists = parse_if_not_exists(self);

	// create synonym config
	stmt->config = synonym_config_allocate();

	// [user.]name
	Str user;
	Str target;
	parse_target(self, &user, &target);
	synonym_config_set_user(stmt->config, &user);
	synonym_config_set_name(stmt->config, &target);

	// TO
	stmt_expect(self, KTO);

	// [user.]name
	parse_target(self, &user, &target);
	synonym_config_set_for_user(stmt->config, &user);
	synonym_config_set_for_name(stmt->config, &target);
}

void
parse_synonym_drop(Stmt* self)
{
	// DROP SYNONYM [IF EXISTS] name
	auto stmt = ast_synonym_drop_allocate();
	self->ast = &stmt->ast;

	// if exists
	stmt->if_exists = parse_if_exists(self);

	// name
	auto name  = stmt_expect(self, KNAME);
	stmt->name = name->string;
}

void
parse_synonym_alter(Stmt* self)
{
	// ALTER SYNONYM [IF EXISTS] name RENAME name
	auto stmt = ast_synonym_alter_allocate();
	self->ast = &stmt->ast;

	// if exists
	stmt->if_exists = parse_if_exists(self);

	// name
	auto name  = stmt_expect(self, KNAME);
	stmt->name = name->string;

	// RENAME
	stmt_expect(self, KRENAME);

	// [TO]
	stmt_if(self, KTO);

	// name
	auto name_new  = stmt_expect(self, KNAME);
	stmt->name_new = name_new->string;
}
