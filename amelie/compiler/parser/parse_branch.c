
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
	// CREATE BRANCH [IF NOT EXISTS] name ON table_name [FROM name]
	auto stmt = ast_branch_create_allocate();
	self->ast = &stmt->ast;

	// if not exists
	stmt->if_not_exists = parse_if_not_exists(self);

	// name
	auto name = stmt_expect(self, KNAME);

	// ON
	stmt_expect(self, KON);

	// table_name
	auto target = stmt_expect(self, KNAME);
	stmt->table_name = target->string;

	// find table
	auto table = catalog_find_table(&share()->db->catalog, self->parser->db,
	                                &stmt->table_name,
	                                false);
	if (! table)
		stmt_error(self, target, "table not found");

	// get max branch id
	auto branches = &table->config->partitioning.branches;
	auto max = 0;
	list_foreach(&branches->list)
	{
		auto branch = list_at(Branch, link);
		if (branch->id > max)
			max = branch->id;
	}

	// create branch config
	auto config = branch_allocate();
	stmt->config = config;
	branch_set_name(config, &name->string);
	branch_set_id(config, max + 1);
	branch_set_id_parent(config, 0);
	branch_set_snapshot(config, state_tsn());

	// [FROM]
	if (stmt_if(self, KFROM))
	{
		name = stmt_expect(self, KNAME);
		auto branch = branch_mgr_find(&table->config->partitioning.branches,
		                              &name->string);
		if (! branch)
			stmt_error(self, name, "branch not found");
		branch_set_id_parent(config, branch->id);
	}
}

void
parse_branch_drop(Stmt* self)
{
	// DROP BRANCH [IF EXISTS] name ON table_name
	auto stmt = ast_branch_drop_allocate();
	self->ast = &stmt->ast;

	// if exists
	stmt->if_exists = parse_if_exists(self);

	// name
	auto name = stmt_expect(self, KNAME);
	stmt->name = name->string;

	// ON
	stmt_expect(self, KON);

	// table
	auto name_table  = stmt_expect(self, KNAME);
	stmt->table_name = name_table->string;
}

void
parse_branch_alter(Stmt* self)
{
	// ALTER BRANCH [IF EXISTS] name ON table_name RENAME TO name
	auto stmt = ast_branch_alter_allocate();
	self->ast = &stmt->ast;

	// if exists
	stmt->if_exists = parse_if_exists(self);

	// name
	auto name = stmt_expect(self, KNAME);
	stmt->name = name->string;

	// ON
	stmt_expect(self, KON);

	// table
	auto name_table  = stmt_expect(self, KNAME);
	stmt->table_name = name_table->string;

	// RENAME
	stmt_expect(self, KRENAME);

	// TO
	stmt_expect(self, KTO);

	// name
	name = stmt_expect(self, KNAME);
	stmt->name_new = name->string;
}
