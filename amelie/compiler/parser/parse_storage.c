
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
parse_storage_create(Stmt* self)
{
	// CREATE STORAGE [IF NOT EXISTS] name [(options)]
	auto stmt = ast_storage_create_allocate();
	self->ast = &stmt->ast;

	// if not exists
	stmt->if_not_exists = parse_if_not_exists(self);

	// create storage config
	stmt->config = storage_config_allocate();

	// name
	auto name = stmt_expect(self, KNAME);
	storage_config_set_name(stmt->config, &name->string);

	// ()
	if (!stmt_if(self, '(') || stmt_if(self, ')'))
		return;

	for (;;)
	{
		// name value
		auto name = stmt_expect(self, KNAME);
		if (str_is(&name->string, "path", 4))
		{
			// path string
			auto path = stmt_expect(self, KSTRING);
			storage_config_set_path(stmt->config, &path->string);
		} else {
			stmt_error(self, name, "unknown option");
		}

		// )
		if (stmt_if(self, ')'))
			break;

		// ,
		stmt_expect(self, ',');
	}
}

void
parse_storage_drop(Stmt* self)
{
	// DROP STORAGE [IF EXISTS] name
	auto stmt = ast_storage_drop_allocate();
	self->ast = &stmt->ast;

	// if exists
	stmt->if_exists = parse_if_exists(self);

	// name
	stmt->name = stmt_expect(self, KNAME);
}

void
parse_storage_alter(Stmt* self)
{
	// ALTER STORAGE [IF EXISTS] name RENAME name
	auto stmt = ast_storage_alter_allocate();
	self->ast = &stmt->ast;

	// if exists
	stmt->if_exists = parse_if_exists(self);

	// name
	stmt->name = stmt_expect(self, KNAME);

	// RENAME
	stmt_expect(self, KRENAME);

	// TO
	stmt_expect(self, KTO);

	// name
	stmt->name_new = stmt_expect(self, KNAME);
}
