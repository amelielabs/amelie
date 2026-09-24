
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
parse_channel_create(Stmt* self)
{
	// CREATE CHANNEL [IF NOT EXISTS] name
	// [ID]
	// [DESCRIPTION]
	// [GRANT]
	auto stmt = ast_channel_create_allocate();
	self->ast = &stmt->ast;

	// if not exists
	stmt->if_not_exists = parse_if_not_exists(self);

	// [user.]name
	Str user;
	Str name;
	parse_target(self, &user, &name);

	// create channel config
	auto config = channel_config_allocate();
	stmt->config = config;
	channel_config_set_user(config, &user);
	channel_config_set_name(config, &name);

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

		// DESCRIPTION string
		if (str_is_case(&name->string, "description", 11))
		{
			auto text = stmt_expect(self, KSTRING);
			channel_config_set_description(stmt->config, &text->string);
			continue;
		}

		// GRANT name, ... TO user
		if (str_is_case(&name->string, "grant", 5))
		{
			parse_grant_to_inline(self, &config->grants);
			continue;
		}

		stmt_error(self, name, "unrecognized option");
	}
}

void
parse_channel_drop(Stmt* self)
{
	// DROP CHANNEL [IF EXISTS] name [CASCADE]
	auto stmt = ast_channel_drop_allocate();
	self->ast = &stmt->ast;

	// if exists
	stmt->if_exists = parse_if_exists(self);

	// [user.]name
	parse_target(self, &stmt->user, &stmt->name);

	// [CASCADE]
	stmt->cascade = stmt_if(self, KCASCADE) != NULL;
}

void
parse_channel_alter(Stmt* self)
{
	// ALTER CHANNEL [IF EXISTS] name RENAME TO name
	// ALTER CHANNEL [IF EXISTS] name DESCRIPTION text
	auto stmt = ast_channel_alter_allocate();
	self->ast = &stmt->ast;

	// if exists
	stmt->if_exists = parse_if_exists(self);

	// [user.]name
	parse_target(self, &stmt->user, &stmt->name);

	// RENAME
	if (stmt_if(self, KRENAME))
	{
		// TO
		stmt_expect(self, KTO);
		stmt->type = CHANNEL_ALTER_RENAME;

		// name
		auto name = stmt_expect(self, KNAME);
		stmt->name_new = name->string;
		return;
	}

	// DESCRIPTION
	if (stmt_if(self, KDESCRIPTION))
	{
		auto text = stmt_expect(self, KSTRING);
		stmt->type = CHANNEL_ALTER_DESCRIPTION;
		stmt->description = text->string;
		return;
	}

	stmt_error(self, NULL, "RENAME or DESCRIPTION expected");
}
