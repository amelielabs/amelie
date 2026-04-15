
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
	// CREATE SUBSCRIPTION [IF NOT EXISTS] name ON [user.]table, ...
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
	Uuid id;
	uuid_init(&id);
	uuid_generate(&id, &runtime()->random);
	sub_config_set_id(config, &id);
	sub_config_set_lsn(config, state_lsn());

	// ON
	stmt_expect(self, KON);

	// [user.]table
	Str user;
	Str target;
	auto path = parse_target(self, &user, &target);
	auto table = catalog_find_table(&share()->db->catalog, &user, &target, false);
	if (! table)
		stmt_error(self, path, "table not found");
	sub_config_set_id_rel(config, &table->config->id);
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

void
parse_sub_alter(Stmt* self)
{
	// ALTER SUBSCRIPTION [IF EXISTS] name RENAME TO name
	auto stmt = ast_sub_alter_allocate();
	self->ast = &stmt->ast;

	// if exists
	stmt->if_exists = parse_if_exists(self);

	// name
	auto name = stmt_expect(self, KNAME);
	stmt->name = name->string;

	// RENAME
	stmt_expect(self, KRENAME);

	// TO
	stmt_expect(self, KTO);

	// name
	name = stmt_expect(self, KNAME);
	stmt->name_new = name->string;
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

	// name
	auto value = stmt_expect(self, KINT);
	stmt->to = value->integer;

	// subscription
	stmt->sub = sub_mgr_find(&share()->db->catalog.sub_mgr, self->parser->user, &name->string, false);
	if (! stmt->sub)
		stmt_error(self, name, "subscription not found");

	// require exclusive lock
	access_add(&self->parser->program->access, &stmt->sub->rel,
	           LOCK_EXCLUSIVE, PERM_SELECT);
}
