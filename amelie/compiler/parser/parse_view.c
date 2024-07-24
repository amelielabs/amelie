
//
// sonata.
//
// Real-Time SQL Database.
//

#include <sonata_runtime.h>
#include <sonata_io.h>
#include <sonata_lib.h>
#include <sonata_data.h>
#include <sonata_config.h>
#include <sonata_user.h>
#include <sonata_http.h>
#include <sonata_client.h>
#include <sonata_server.h>
#include <sonata_row.h>
#include <sonata_transaction.h>
#include <sonata_index.h>
#include <sonata_partition.h>
#include <sonata_wal.h>
#include <sonata_db.h>
#include <sonata_value.h>
#include <sonata_aggr.h>
#include <sonata_executor.h>
#include <sonata_vm.h>
#include <sonata_parser.h>

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
		if (select->expr_count != stmt->config->columns.list_count)
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
