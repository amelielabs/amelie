
//
// monotone
//
// SQL OLTP database
//

#include <monotone_runtime.h>
#include <monotone_io.h>
#include <monotone_data.h>
#include <monotone_lib.h>
#include <monotone_config.h>
#include <monotone_auth.h>
#include <monotone_def.h>
#include <monotone_transaction.h>
#include <monotone_storage.h>
#include <monotone_wal.h>
#include <monotone_db.h>
#include <monotone_value.h>
#include <monotone_aggr.h>
#include <monotone_request.h>
#include <monotone_vm.h>
#include <monotone_parser.h>

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
	// ALTER SCHEMA [IF EXISTS] name RENAME TO name
	auto stmt = ast_schema_alter_allocate();
	self->ast = &stmt->ast;

	// prepare schema config
	stmt->config = schema_config_allocate();

	// if exists
	stmt->if_exists = parse_if_exists(self);

	// name
	stmt->name = stmt_if(self, KNAME);
	if (! stmt->name)
		error("ALTER SCHEMA <name> expected");

	// RENAME
	if (! stmt_if(self, KRENAME))
		error("ALTER SCHEMA <RENAME> expected");

	// TO
	if (! stmt_if(self, KTO))
		error("ALTER SCHEMA RENAME <TO> expected");

	// name
	auto name_new = stmt_if(self, KNAME);
	if (! name_new)
		error("ALTER SCHEMA RENAME TO <name> expected");
	schema_config_set_name(stmt->config, &name_new->string);
}
