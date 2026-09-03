
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
parse_sub_create(Stmt* self)
{
	// CREATE SUBSCRIPTION [IF NOT EXISTS] name ON [USER] [user.]relation
	// [DESCRIPTION]
	// [LSN]
	// [GRANT]
	auto stmt = ast_sub_create_allocate();
	self->ast = &stmt->ast;

	// if not exists
	stmt->if_not_exists = parse_if_not_exists(self);

	// [user.]name
	Str user;
	Str name;
	parse_target(self, &user, &name);

	// ON
	stmt_expect(self, KON);

	Str target_user;
	Str target;
	if (stmt_if(self, KUSER) || stmt_if(self, KAGENT))
	{
		// USER|AGENT name
		auto user_name = stmt_expect(self, KNAME);
		target_user = user_name->string;
		str_init(&target);
	} else
	{
		// [user.]relation
		parse_target(self, &target_user, &target);
	}

	// create subscription config
	auto config = sub_config_allocate();
	stmt->config = config;
	sub_config_set_user(config, &user);
	sub_config_set_name(config, &name);
	sub_config_set_rel_user(config, &target_user);
	sub_config_set_rel(config, &target);
	sub_config_set_pos(config, state_lsn() + 1);

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
			sub_config_set_description(stmt->config, &text->string);
			continue;
		}

		// LSN int
		if (str_is_case(&name->string, "lsn", 3))
		{
			auto value = stmt_expect(self, KINT);
			if (value->integer <= (int64_t)state_lsn())
				stmt_error(self, value, "invalid lsn");
			sub_config_set_pos(config, value->integer);
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
parse_sub_drop(Stmt* self)
{
	// DROP SUBSCRIPTION [IF EXISTS] name [CASCADE]
	auto stmt = ast_sub_drop_allocate();
	self->ast = &stmt->ast;

	// if exists
	stmt->if_exists = parse_if_exists(self);

	// [user.]name
	parse_target(self, &stmt->user, &stmt->name);

	// [CASCADE]
	stmt->cascade = stmt_if(self, KCASCADE) != NULL;
}

void
parse_sub_alter(Stmt* self)
{
	// ALTER SUBSCRIPTION [IF EXISTS] name RENAME TO name
	// ALTER SUBSCRIPTION [IF EXISTS] name DESCRIPTION text
	auto stmt = ast_sub_alter_allocate();
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
		stmt->type = SUBSCRIPTION_ALTER_RENAME;

		// name
		auto name = stmt_expect(self, KNAME);
		stmt->name_new = name->string;
		return;
	}

	// DESCRIPTION
	if (stmt_if(self, KDESCRIPTION))
	{
		auto text = stmt_expect(self, KSTRING);
		stmt->type = SUBSCRIPTION_ALTER_DESCRIPTION;
		stmt->description = text->string;
		return;
	}

	stmt_error(self, NULL, "RENAME or DESCRIPTION expected");
}

void
parse_acknowledge(Stmt* self)
{
	// ACKNOWLEDGE name TO value
	auto stmt = ast_ack_allocate();
	self->ast = &stmt->ast;

	// [user.]name
	auto target = parse_target(self, &stmt->user, &stmt->name);

	// TO
	stmt_expect(self, KTO);

	// lsn
	auto value = stmt_expect(self, KINT);
	stmt->lsn = value->integer;

	// subscription
	stmt->sub = catalog_find_sub(&share()->db->catalog, &stmt->user, &stmt->name, false);
	if (! stmt->sub)
		stmt_error(self, target, "subscription not found");

	// require exclusive lock
	access_add(&self->parser->program->access, &stmt->sub->rel,
	           LOCK_EXCLUSIVE, PERM_SELECT);
}
