
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
	// CREATE SUBSCRIPTION [IF NOT EXISTS] name ON [user.]channel, ...
	auto stmt = ast_sub_create_allocate();
	self->ast = &stmt->ast;

	// if not exists
	stmt->if_not_exists = parse_if_not_exists(self);

	// name
	auto name = stmt_expect(self, KNAME);

	// create subscription config
	auto config = sub_config_allocate();
	stmt->config = config;
	sub_config_set_user(config, self->parser->user);
	sub_config_set_name(config, &name->string);

	// ON
	stmt_expect(self, KON);

	// [user.]channel, ...
	for (;;)
	{
		Str user;
		Str target;
		auto path = parse_target(self, &user, &target);
		auto channel = channel_mgr_find(&share()->db->catalog.channel_mgr, &user, &target, false);
		if (! channel)
			stmt_error(self, path, "channel not found");
		buf_write(&config->channels, &channel->config->id, sizeof(Uuid));
		if (stmt_if(self, ','))
			continue;
		break;
	}
}

void
parse_sub_drop(Stmt* self)
{
	// DROP SUBSCRIPTION [IF EXISTS] name
	auto stmt = ast_sub_drop_allocate();
	self->ast = &stmt->ast;

	// if exists
	stmt->if_exists = parse_if_exists(self);

	// name
	auto name = stmt_expect(self, KNAME);
	stmt->name = name->string;
}
