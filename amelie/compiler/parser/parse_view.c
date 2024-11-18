
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

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_data.h>
#include <amelie_config.h>
#include <amelie_user.h>
#include <amelie_http.h>
#include <amelie_client.h>
#include <amelie_server.h>
#include <amelie_row.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_value.h>
#include <amelie_store.h>
#include <amelie_executor.h>
#include <amelie_vm.h>
#include <amelie_parser.h>

static void
parse_view_args(Stmt* self, AstViewCreate* stmt)
{
	// )
	if (stmt_if(self, ')'))
		return;

	for (;;)
	{
		// name
		auto name = stmt_if(self, KNAME);
		if (! name)
			error("CREATE VIEW (<name> expected");

		// ensure arg does not exists
		auto arg = columns_find(&stmt->config->columns, &name->string);
		if (arg)
			error("<%.*s> view argument redefined", str_size(&name->string),
			      str_of(&name->string));

		// add argument
		arg = column_allocate();
		columns_add(&stmt->config->columns, arg);
		column_set_name(arg, &name->string);
		encode_null(&arg->constraint.value);

		// ,
		if (! stmt_if(self, ','))
			break;
	}

	// )
	if (! stmt_if(self, ')'))
		error("CREATE VIEW (<)> expected");
}

void
parse_view_create(Stmt* self)
{
	// CREATE VIEW [IF NOT EXISTS] name [(args)]
	// AS SELECT
	auto stmt = ast_view_create_allocate();
	self->ast = &stmt->ast;

	// if not exists
	stmt->if_not_exists = parse_if_not_exists(self);

	// create view config
	stmt->config = view_config_allocate();

	// name
	Str schema;
	Str name;
	if (! parse_target(self, &schema, &name))
		error("CREATE VIEW <name> expected");
	view_config_set_schema(stmt->config, &schema);
	view_config_set_name(stmt->config, &name);

	// (args)
	if (stmt_if(self, '('))
		parse_view_args(self, stmt);

	// AS
	if (! stmt_if(self, KAS))
		error("CREATE VIEW <AS> expected");

	// SELECT
	char* start = self->lex->pos;
	if (! stmt_if(self, KSELECT))
		error("CREATE VIEW AS <SELECT> expected");

	// ensure column count match
	auto select = parse_select(self);
	if (stmt->config->columns.list_count > 0)
		if (select->ret.count != stmt->config->columns.list_count)
			error("number of view columns does not match select");

	Str query;
	str_init(&query);
	str_set(&query, start, self->lex->pos - start);
	str_shrink(&query);
	view_config_set_query(stmt->config, &query);
}

void
parse_view_drop(Stmt* self)
{
	// DROP VIEW [IF EXISTS] name
	auto stmt = ast_view_drop_allocate();
	self->ast = &stmt->ast;

	// if exists
	stmt->if_exists = parse_if_exists(self);

	// name
	if (! parse_target(self, &stmt->schema, &stmt->name))
		error("DROP VIEW <name> expected");
}

void
parse_view_alter(Stmt* self)
{
	// ALTER VIEW [IF EXISTS] [schema.]name RENAME [schema.]name
	auto stmt = ast_view_alter_allocate();
	self->ast = &stmt->ast;

	// if exists
	stmt->if_exists = parse_if_exists(self);

	// name
	if (! parse_target(self, &stmt->schema, &stmt->name))
		error("ALTER VIEW <name> expected");

	// RENAME
	if (! stmt_if(self, KRENAME))
		error("ALTER VIEW <RENAME> expected");

	// [TO]
	stmt_if(self, KTO);

	// name
	if (! parse_target(self, &stmt->schema_new, &stmt->name_new))
		error("ALTER VIEW RENAME <name> expected");
}
