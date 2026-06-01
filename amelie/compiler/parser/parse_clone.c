
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
parse_clone_create(Stmt* self)
{
	// CREATE CLONE [IF NOT EXISTS] name OF relation [DESCRIPTION text]
	auto stmt = ast_clone_create_allocate();
	self->ast = &stmt->ast;

	// if not exists
	stmt->if_not_exists = parse_if_not_exists(self);

	// name
	auto name = stmt_expect(self, KNAME);

	// OF
	auto ast = stmt_next_shadow(self);
	if (ast->id != KNAME ||
	    (!str_is_case(&ast->string, "OF", 2) &&
	     !str_is_case(&ast->string, "FROM", 4)))
		stmt_error(self, ast, "OF expected");

	// target
	Str target_user;
	Str target;
	auto path = parse_target(self, &target_user, &target);

	// find target
	auto table = catalog_find_table(&share()->db->catalog, &target_user, &target, true);
	if (! table)
		stmt_error(self, path, "table not found");

	// calculate clone id
	uint32_t id = snapshot_mgr_max(&table->snapshot_mgr);
	list_foreach(&table->part_mgr.list)
	{
		auto part = list_at(Part, link);
		if (part->heap->header->ssn > id)
			id = part->heap->header->ssn;
	}
	id++;

	// create clone config
	auto config = clone_config_allocate();
	stmt->config = config;
	clone_config_set_user(config, self->parser->user);
	clone_config_set_name(config, &name->string);
	clone_config_set_table_user(config, &table->config->user);
	clone_config_set_table(config, &table->config->name);
	Uuid uuid;
	uuid_init(&uuid);
	uuid_generate(&uuid, &runtime()->random);
	clone_config_set_id(config, &uuid);

	// set clone snapshot
	auto snapshot = &config->snapshot;
	snapshot_set_id(snapshot, id);
	snapshot_set_snapshot(snapshot, state_tsn());

	// [DESCRIPTION]
	auto description = stmt_if(self, KDESCRIPTION);
	if (description)
	{
		auto text = stmt_expect(self, KSTRING);
		clone_config_set_description(stmt->config, &text->string);
	}
}

void
parse_clone_drop(Stmt* self)
{
	// DROP CLONE [IF EXISTS] name [CASCADE]
	auto stmt = ast_clone_drop_allocate();
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
parse_clone_alter(Stmt* self)
{
	// ALTER CLONE [IF EXISTS] name RENAME TO name
	auto stmt = ast_clone_alter_allocate();
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
		stmt->type = CLONE_ALTER_RENAME;

		// name
		name = stmt_expect(self, KNAME);
		stmt->name_new = name->string;
		return;
	}

	// DESCRIPTION
	if (stmt_if(self, KDESCRIPTION))
	{
		auto text = stmt_expect(self, KSTRING);
		stmt->type = CLONE_ALTER_DESCRIPTION;
		stmt->description = text->string;
		return;
	}

	stmt_error(self, NULL, "RENAME or DESCRIPTION expected");
}
