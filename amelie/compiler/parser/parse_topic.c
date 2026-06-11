
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
parse_topic_create(Stmt* self, bool unlogged)
{
	// CREATE [UNLOGGED] TOPIC [IF NOT EXISTS] name [DESCRIPTION text]
	auto stmt = ast_topic_create_allocate();
	self->ast = &stmt->ast;

	// if not exists
	stmt->if_not_exists = parse_if_not_exists(self);

	// name
	auto name = stmt_expect(self, KNAME);

	// create topic config
	auto config = topic_config_allocate();
	stmt->config = config;
	topic_config_set_user(config, self->parser->user);
	topic_config_set_name(config, &name->string);
	Uuid id;
	uuid_init(&id);
	uuid_generate(&id, &self->parser->local->random);
	topic_config_set_id(config, &id);
	topic_config_set_unlogged(config, unlogged);

	// [DESCRIPTION]
	auto description = stmt_if(self, KDESCRIPTION);
	if (description)
	{
		auto text = stmt_expect(self, KSTRING);
		topic_config_set_description(stmt->config, &text->string);
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

	// name
	auto name = stmt_expect(self, KNAME);
	stmt->name = name->string;

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

	// name
	auto name = stmt_expect(self, KNAME);
	stmt->name = name->string;

	// RENAME
	if (stmt_if(self, KRENAME))
	{
		// TO
		stmt_expect(self, KTO);
		stmt->type = TOPIC_ALTER_RENAME;

		// name
		name = stmt_expect(self, KNAME);
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
