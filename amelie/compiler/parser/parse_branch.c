
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
parse_branch_create(Stmt* self)
{
	// CREATE BRANCH [IF NOT EXISTS] name FROM relation
	auto stmt = ast_branch_create_allocate();
	self->ast = &stmt->ast;

	// if not exists
	stmt->if_not_exists = parse_if_not_exists(self);

	// name
	auto name = stmt_expect(self, KNAME);

	// FROM
	stmt_expect(self, KFROM);

	// target
	Str target_user;
	Str target;
	auto path = parse_target(self, &target_user, &target);

	// find target
	auto rel = catalog_find(&share()->db->catalog, &target_user, &target, false);
	if (! rel)
		stmt_error(self, path, "relation not found");

	Table*    table  = NULL;
	Snapshot* parent = NULL;
	switch (rel->type) {
	case REL_TABLE:
	{
		table  = table_of(rel);
		parent = table_main(table);
		break;
	}
	case REL_BRANCH:
	{
		auto branch = branch_of(rel);
		table  = branch->table;
		parent = &branch->config->snapshot;
		break;
	}
	default:
	{
		stmt_error(self, path, "unsupported relation");
		break;
	}
	}

	// request for BRANCH permissions
	access_add(&self->parser->program->access, rel,
	           LOCK_NONE, PERM_BRANCH);

	// calculate branch id
	uint32_t id = snapshot_mgr_max(&table->snapshot_mgr);
	list_foreach(&table->part_mgr.list)
	{
		auto part = list_at(Part, link);
		if (part->heap->header->ssn > id)
			id = part->heap->header->ssn;
	}
	id++;

	// create branch config
	auto config = branch_config_allocate();
	stmt->config = config;
	branch_config_set_user(config, self->parser->user);
	branch_config_set_name(config, &name->string);
	branch_config_set_table_user(config, &table->config->user);
	branch_config_set_table(config, &table->config->name);

	// set branch snapshot
	auto snapshot = &config->snapshot;
	snapshot_set_id(snapshot, id);
	snapshot_set_id_parent(snapshot, parent->id);
	snapshot_set_snapshot(snapshot, state_tsn());
}

void
parse_branch_drop(Stmt* self)
{
	// DROP BRANCH [IF EXISTS] name
	auto stmt = ast_branch_drop_allocate();
	self->ast = &stmt->ast;

	// if exists
	stmt->if_exists = parse_if_exists(self);

	// name
	auto name = stmt_expect(self, KNAME);
	stmt->name = name->string;
}

void
parse_branch_alter(Stmt* self)
{
	// ALTER BRANCH [IF EXISTS] name RENAME TO name
	auto stmt = ast_branch_alter_allocate();
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
