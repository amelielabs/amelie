
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
		auto arg = def_find_column(&stmt->config->def, &name->string);
		if (arg)
			error("<%.*s> view argument redefined", str_size(&name->string),
			      str_of(&name->string));

		// add argument
		arg = column_allocate();
		def_add_column(&stmt->config->def, arg);
		column_set_name(arg, &name->string);

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
	if (stmt->config->def.column_count > 0)
		if (select->expr_count != stmt->config->def.column_count)
			error("view columns count does not match select");

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
	// ALTER VIEW [IF EXISTS] name [RENAME TO name]
	// ALTER VIEW [IF EXISTS] name [SET SCHEMA name]
	(void)self;
#if 0
	auto stmt = ast_table_alter_allocate();
	self->ast = &stmt->ast;

	// TODO: copy config

	// if exists
	stmt->if_exists = parse_if_exists(self);

	// name
	if (! parse_target(self, &stmt->schema, &stmt->name))
		error("ALTER TABLE <name> expected");

	// RENAME TO
	if (stmt_if(self, KRENAME))
	{
		if (! stmt_if(self, KTO))
			error("ALTER TABLE RENAME <TO> expected");

		// todo
	} else
	if (stmt_if(self, KSET))
	{
		if (! stmt_if(self, KSCHEMA))
			error("ALTER TABLE SET <SCHEMA> expected");

		// todo
	}
#endif
}
