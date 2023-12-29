
//
// indigo
//
// SQL OLTP database
//

#include <indigo_runtime.h>
#include <indigo_io.h>
#include <indigo_data.h>
#include <indigo_lib.h>
#include <indigo_config.h>
#include <indigo_auth.h>
#include <indigo_def.h>
#include <indigo_transaction.h>
#include <indigo_index.h>
#include <indigo_storage.h>
#include <indigo_wal.h>
#include <indigo_db.h>
#include <indigo_value.h>
#include <indigo_aggr.h>
#include <indigo_request.h>
#include <indigo_vm.h>
#include <indigo_parser.h>

void
parse_schema_create(Stmt* self)
{
	// CREATE SCHEMA [IF NOT EXISTS] name
	auto stmt = ast_schema_create_allocate();
	self->ast = &stmt->ast;

	// if not exists
	stmt->if_not_exists = parse_if_not_exists(self);

	// create schema config
	stmt->config = schema_config_allocate();
	schema_config_set_system(stmt->config, false);
	schema_config_set_create(stmt->config, true);

	// name
	auto name = stmt_if(self, KNAME);
	if (! name)
		error("CREATE SCHEMA <name> expected");
	schema_config_set_name(stmt->config, &name->string);
}

void
parse_schema_drop(Stmt* self)
{
	// DROP SCHEMA [IF EXISTS] name
	auto stmt = ast_schema_drop_allocate();
	self->ast = &stmt->ast;

	// if exists
	stmt->if_exists = parse_if_exists(self);

	// name
	stmt->name = stmt_if(self, KNAME);
	if (! stmt->name)
		error("DROP SCHEMA <name> expected");

	// [CASCADE]
	if (stmt_if(self, KCASCADE))
		stmt->cascade = true;
}

void
parse_schema_alter(Stmt* self)
{
	// ALTER SCHEMA [IF EXISTS] name RENAME name
	auto stmt = ast_schema_alter_allocate();
	self->ast = &stmt->ast;

	// if exists
	stmt->if_exists = parse_if_exists(self);

	// name
	stmt->name = stmt_if(self, KNAME);
	if (! stmt->name)
		error("ALTER SCHEMA <name> expected");

	// RENAME
	if (! stmt_if(self, KRENAME))
		error("ALTER SCHEMA <RENAME> expected");

	// [TO]
	stmt_if(self, KTO);

	// name
	stmt->name_new = stmt_if(self, KNAME);
	if (! stmt->name_new)
		error("ALTER SCHEMA RENAME <name> expected");
}
