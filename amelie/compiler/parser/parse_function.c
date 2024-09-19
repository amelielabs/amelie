
//
// amelie.
//
// Real-Time SQL Database.
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
#include <amelie_aggr.h>
#include <amelie_executor.h>
#include <amelie_vm.h>
#include <amelie_parser.h>

static void
parse_function_args(Stmt* self, AstFunctionCreate* stmt)
{
	// )
	if (stmt_if(self, ')'))
		return;

	for (;;)
	{
		// name
		auto name = stmt_if(self, KNAME);
		if (! name)
			error("CREATE FUNCTION (<name> expected");

		// ensure arg does not exists
		auto arg = columns_find(&stmt->config->columns, &name->string);
		if (arg)
			error("<%.*s> function argument redefined", str_size(&name->string),
			      str_of(&name->string));

		// add argument
		arg = column_allocate();
		columns_add(&stmt->config->columns, arg);
		column_set_name(arg, &name->string);
		encode_null(&arg->constraint.value);

		// type
		auto type = parse_type(self, arg, NULL);
		column_set_type(arg, type);

		// ,
		if (! stmt_if(self, ','))
			break;
	}

	// )
	if (! stmt_if(self, ')'))
		error("CREATE FUNCTION (<)> expected");
}

void
parse_function_create(Stmt* self, bool or_replace)
{
	// CREATE [OR REPLACE] FUNCTION [schema.]name [(args)]
	// BEGIN
	//  [stmt[; stmt]]
	// END
	auto stmt = ast_function_create_allocate();
	self->ast = &stmt->ast;

	// or replace
	stmt->or_replace = or_replace;

	// create function config
	stmt->config = udf_config_allocate();

	// name
	Str schema;
	Str name;
	if (! parse_target(self, &schema, &name))
		error("CREATE FUNCTION <name> expected");
	udf_config_set_schema(stmt->config, &schema);
	udf_config_set_name(stmt->config, &name);

	// (args)
	if (stmt_if(self, '('))
		parse_function_args(self, stmt);

	// BEGIN
	if (! stmt_if(self, KBEGIN))
		error("CREATE FUNCTION <BEGIN> expected");

	char* start = self->lex->pos;
	char* end   = NULL;

	// match END
	for (;;)
	{
		end = self->lex->pos;
		auto ast = stmt_next(self);
		if (ast->id == KEOF)
			error("CREATE FUNCTION BEGIN ... <END> expected");
		if (ast->id == KEND)
			break;
	}

	Str text;
	str_init(&text);
	str_set(&text, start, end - start);
	str_shrink(&text);
	udf_config_set_text(stmt->config, &text);
}

void
parse_function_drop(Stmt* self)
{
	// DROP FUNCTION [IF EXISTS] name
	auto stmt = ast_function_drop_allocate();
	self->ast = &stmt->ast;

	// if exists
	stmt->if_exists = parse_if_exists(self);

	// name
	if (! parse_target(self, &stmt->schema, &stmt->name))
		error("DROP FUNCTION <name> expected");
}

void
parse_function_alter(Stmt* self)
{
	// ALTER FUNCTION [IF EXISTS] [schema.]name RENAME [schema.]name
	auto stmt = ast_function_alter_allocate();
	self->ast = &stmt->ast;

	// if exists
	stmt->if_exists = parse_if_exists(self);

	// name
	if (! parse_target(self, &stmt->schema, &stmt->name))
		error("ALTER FUNCTION <name> expected");

	// RENAME
	if (! stmt_if(self, KRENAME))
		error("ALTER FUNCTION <RENAME> expected");

	// [TO]
	stmt_if(self, KTO);

	// name
	if (! parse_target(self, &stmt->schema_new, &stmt->name_new))
		error("ALTER FUNCTION RENAME <name> expected");
}
