
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
#include <amelie_json.h>
#include <amelie_config.h>
#include <amelie_user.h>
#include <amelie_auth.h>
#include <amelie_http.h>
#include <amelie_client.h>
#include <amelie_server.h>
#include <amelie_row.h>
#include <amelie_heap.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_backup.h>
#include <amelie_repl.h>
#include <amelie_value.h>
#include <amelie_set.h>
#include <amelie_content.h>
#include <amelie_executor.h>
#include <amelie_func.h>
#include <amelie_vm.h>
#include <amelie_parser.h>

static void
parse_procedure_args(Stmt* self, AstProcedureCreate* stmt)
{
	// ()
	stmt_expect(self, '(');
	if (stmt_if(self, ')'))
		return;

	for (;;)
	{
		// name
		auto name = stmt_expect(self, KNAME);

		// ensure arg does not exists
		auto arg = columns_find(&stmt->config->columns, &name->string);
		if (arg)
			stmt_error(self, name, "argument redefined");

		// add argument
		arg = column_allocate();
		column_set_name(arg, &name->string);
		encode_null(&arg->constraints.value);
		columns_add(&stmt->config->columns, arg);

		// type
		int type_size;
		int type;
		if (parse_type(self, &type, &type_size))
			stmt_error(self, name, "serial type cannot be used here");
		column_set_type(arg, type, type_size);
		// ,
		if (! stmt_if(self, ','))
			break;
	}

	// )
	stmt_expect(self, ')');
}

static void
parse_procedure_vars(Stmt* self, AstProcedureCreate* stmt)
{
	for (;;)
	{
		// name
		auto name = stmt_if(self, KNAME);
		if (! name)
			break;

		// ensure arg or variable does not exists
		auto var = columns_find(&stmt->config->columns, &name->string);
		if (var)
			stmt_error(self, name, "argument redefined");

		var = columns_find(&stmt->config->vars, &name->string);
		if (var)
			stmt_error(self, name, "variable redefined");

		// add variable
		var = column_allocate();
		column_set_name(var, &name->string);
		encode_null(&var->constraints.value);
		columns_add(&stmt->config->vars, var);

		// type
		int type_size;
		int type;
		if (parse_type(self, &type, &type_size))
			stmt_error(self, name, "serial type cannot be used here");
		column_set_type(var, type, type_size);

		// ;
		stmt_expect(self, ';');
	}
}

void
parse_procedure_create(Stmt* self, bool or_replace)
{
	// CREATE [OR REPLACE] PROCEDURE [schema.]name (args)
	// [DECLARE]
	//   [var type ;]
	// BEGIN
	//  [stmt[; stmt]]
	// END
	auto stmt = ast_procedure_create_allocate();
	self->ast = &stmt->ast;

	// or replace
	stmt->or_replace = or_replace;

	// create procedure config
	stmt->config = proc_config_allocate();

	// name
	Str schema;
	Str name;
	if (! parse_target(self, &schema, &name))
		stmt_error(self, NULL, "name expected");
	proc_config_set_schema(stmt->config, &schema);
	proc_config_set_name(stmt->config, &name);

	// (args)
	parse_procedure_args(self, stmt);

	// [DECLARE]
	if (stmt_if(self, KDECLARE))
		parse_procedure_vars(self, stmt);

	// BEGIN
	stmt_expect(self, KBEGIN);

	char* start = self->lex->pos;
	char* end   = NULL;

	// match END
	for (;;)
	{
		end = self->lex->pos;
		auto ast = stmt_next(self);
		if (ast->id == KEOF)
			stmt_error(self, ast, "END expected");
		if (ast->id == KEND)
			break;
	}

	Str text;
	str_init(&text);
	str_set(&text, start, end - start);
	str_shrink(&text);
	proc_config_set_text(stmt->config, &text);
}

void
parse_procedure_drop(Stmt* self)
{
	// DROP PROCEDURE [IF EXISTS] name
	auto stmt = ast_procedure_drop_allocate();
	self->ast = &stmt->ast;

	// if exists
	stmt->if_exists = parse_if_exists(self);

	// name
	if (! parse_target(self, &stmt->schema, &stmt->name))
		stmt_error(self, NULL, "name expected");
}

void
parse_procedure_alter(Stmt* self)
{
	// ALTER PROCEDURE [IF EXISTS] [schema.]name RENAME [schema.]name
	auto stmt = ast_procedure_alter_allocate();
	self->ast = &stmt->ast;

	// if exists
	stmt->if_exists = parse_if_exists(self);

	// name
	if (! parse_target(self, &stmt->schema, &stmt->name))
		stmt_error(self, NULL, "name expected");

	// RENAME
	stmt_expect(self, KRENAME);

	// [TO]
	stmt_if(self, KTO);

	// name
	if (! parse_target(self, &stmt->schema_new, &stmt->name_new))
		stmt_error(self, NULL, "name expected");
}
