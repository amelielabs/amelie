
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
parse_sub_create(Stmt* self)
{
	// CREATE SUBSCRIPTION [IF NOT EXISTS] name ON [user.]relation
	// [DESCRIPTION text]
	auto stmt = ast_sub_create_allocate();
	self->ast = &stmt->ast;

	// if not exists
	stmt->if_not_exists = parse_if_not_exists(self);

	// name
	auto name = stmt_expect(self, KNAME);

	// ON
	stmt_expect(self, KON);

	// [user.]relation
	Str user;
	Str target;
	parse_target(self, &user, &target);

	// create subscription config
	auto config = sub_config_allocate();
	stmt->config = config;
	sub_config_set_user(config, self->parser->user);
	sub_config_set_name(config, &name->string);
	Uuid id;
	uuid_init(&id);
	uuid_generate(&id, &self->parser->local->random);
	sub_config_set_id(config, &id);
	sub_config_set_rel_user(config, &user);
	sub_config_set_rel(config, &target);
	sub_config_set_pos(config, state_lsn() + 1, 0);

	// [DESCRIPTION]
	auto description = stmt_if(self, KDESCRIPTION);
	if (description)
	{
		auto text = stmt_expect(self, KSTRING);
		sub_config_set_description(stmt->config, &text->string);
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

	// name
	auto name = stmt_expect(self, KNAME);
	stmt->name = name->string;

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

	// name
	auto name = stmt_expect(self, KNAME);
	stmt->name = name->string;

	// RENAME
	if (stmt_if(self, KRENAME))
	{
		// TO
		stmt_expect(self, KTO);
		stmt->type = SUBSCRIPTION_ALTER_RENAME;

		// name
		name = stmt_expect(self, KNAME);
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

	// name
	auto name = stmt_expect(self, KNAME);
	stmt->name = name->string;

	// TO
	stmt_expect(self, KTO);

	// lsn
	auto value = stmt_expect(self, KINT);
	stmt->to_lsn = value->integer;
	stmt->to_op  = 0;

	// [, op]
	if (stmt_if(self, ','))
	{
		value = stmt_expect(self, KINT);
		stmt->to_op = value->integer;
	}

	// subscription
	stmt->sub = catalog_find_sub(&share()->db->catalog, self->parser->user, &name->string, false);
	if (! stmt->sub)
		stmt_error(self, name, "subscription not found");

	// require exclusive lock
	access_add(&self->parser->program->access, &stmt->sub->rel,
	           LOCK_EXCLUSIVE, PERM_SELECT);
}
