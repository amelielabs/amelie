
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
parse_topic_create(Stmt* self)
{
	// CREATE TOPIC [IF NOT EXISTS] name
	// [ID]
	// [DESCRIPTION]
	// [GRANT]
	auto stmt = ast_topic_create_allocate();
	self->ast = &stmt->ast;

	// if not exists
	stmt->if_not_exists = parse_if_not_exists(self);

	// [user.]name
	Str user;
	Str name;
	parse_target(self, &user, &name);

	// create topic config
	auto config = topic_config_allocate();
	stmt->config = config;
	topic_config_set_user(config, &user);
	topic_config_set_name(config, &name);

	Uuid id;
	uuid_init(&id);
	auto local = self->parser->local;
	uuid_generate(&id, &local->random, local->time_ms);
	topic_config_set_id(config, &id);

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
			topic_config_set_id(config, &id);
			continue;
		}

		// DESCRIPTION string
		if (str_is_case(&name->string, "description", 11))
		{
			auto text = stmt_expect(self, KSTRING);
			topic_config_set_description(stmt->config, &text->string);
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
parse_topic_drop(Stmt* self)
{
	// DROP TOPIC [IF EXISTS] name [CASCADE]
	auto stmt = ast_topic_drop_allocate();
	self->ast = &stmt->ast;

	// if exists
	stmt->if_exists = parse_if_exists(self);

	// [user.]name
	parse_target(self, &stmt->user, &stmt->name);

	// [CASCADE]
	stmt->cascade = stmt_if(self, KCASCADE) != NULL;
}

void
parse_topic_alter(Stmt* self)
{
	// ALTER TOPIC [IF EXISTS] name RENAME TO name
	// ALTER TOPIC [IF EXISTS] name DESCRIPTION text
	auto stmt = ast_topic_alter_allocate();
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
		stmt->type = TOPIC_ALTER_RENAME;

		// name
		auto name = stmt_expect(self, KNAME);
		stmt->name_new = name->string;
		return;
	}

	// DESCRIPTION
	if (stmt_if(self, KDESCRIPTION))
	{
		auto text = stmt_expect(self, KSTRING);
		stmt->type = TOPIC_ALTER_DESCRIPTION;
		stmt->description = text->string;
		return;
	}

	stmt_error(self, NULL, "RENAME or DESCRIPTION expected");
}
