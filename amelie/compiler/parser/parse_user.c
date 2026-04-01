
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
parse_user_create(Stmt* self, bool agent)
{
	// CREATE USER|AGENT [IF NOT EXISTS] name
	auto stmt = ast_user_create_allocate();
	self->ast = &stmt->ast;

	// if not exists
	stmt->if_not_exists = parse_if_not_exists(self);

	// create user config
	stmt->config = user_config_allocate();

	// name
	auto name = stmt_expect(self, KNAME);
	user_config_set_name(stmt->config, &name->string);
	user_config_set_agent(stmt->config, agent);

	// set timestamp
	char ts[64];
	auto time = time_us();
	auto size = timestamp_get(time, runtime()->timezone, ts, sizeof(ts));
	Str created_at;
	str_set(&created_at, ts, size);
	user_config_set_created_at(stmt->config, &created_at);

	// set default grants
	auto perms_all =
	     PERM_GRANT           |
	     PERM_CREATE_TOKEN    |
	     PERM_CREATE_TABLE    |
	     PERM_CREATE_FUNCTION;
	Str user;
	str_set_cstr(&user, "self");
	grants_add(&stmt->config->grants, &user, perms_all);
}

void
parse_user_drop(Stmt* self)
{
	// DROP USER|AGENT [IF EXISTS] name [CASCADE]
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
	// ALTER USER|AGENT [IF EXISTS] name RENAME name
	// ALTER USER|AGENT [IF EXISTS] name REVOKE TOKEN
	auto stmt = ast_user_alter_allocate();
	self->ast = &stmt->ast;

	// if exists
	stmt->if_exists = parse_if_exists(self);

	// name
	stmt->name = stmt_expect(self, KNAME);

	// RENAME | REVOKE
	if (stmt_if(self, KRENAME))
	{
		// RENAME
		stmt->type = USER_ALTER_RENAME;

		// TO
		stmt_expect(self, KTO);

		// name
		stmt->name_new = stmt_expect(self, KNAME);
	} else
	if (stmt_if(self, KREVOKE))
	{
		// TOKEN
		stmt_expect(self, KTOKEN);

		// REVOKE
		stmt->type = USER_ALTER_REVOKE;

		// set timestamp
		auto ts = palloc(64);
		auto time = time_us();
		auto size = timestamp_get(time, runtime()->timezone, ts, 64);
		str_set(&stmt->revoked_at, ts, size);
	} else
	{
		stmt_error(self, NULL, "RENAME or REVOKE expected");
	}
}
