
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
parse_user_create(Stmt* self)
{
	// CREATE USER [IF NOT EXISTS] name [SECRET value]
	auto stmt = ast_user_create_allocate();
	self->ast = &stmt->ast;

	// if not exists
	stmt->if_not_exists = parse_if_not_exists(self);

	// create user config
	stmt->config = user_config_allocate();

	// name
	auto name = stmt_expect(self, KNAME);
	user_config_set_name(stmt->config, &name->string);

	// [SECRET]
	if (stmt_if(self, KSECRET))
	{
		// value
		auto value = stmt_expect(self, KSTRING);
		user_config_set_secret(stmt->config, &value->string);
	}
}

void
parse_user_drop(Stmt* self)
{
	// DROP USER [IF EXISTS] name [CASCADE]
	auto stmt = ast_user_drop_allocate();
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
parse_user_alter(Stmt* self)
{
	// ALTER USER [IF EXISTS] name RENAME name
	auto stmt = ast_user_alter_allocate();
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
