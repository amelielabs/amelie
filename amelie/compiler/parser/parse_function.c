
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

#include <amelie_base.h>
#include <amelie_os.h>
#include <amelie_lib.h>
#include <amelie_json.h>
#include <amelie_runtime.h>
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
#include <amelie_catalog.h>
#include <amelie_wal.h>
#include <amelie_storage.h>
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
parse_function_args(Stmt* self, Columns* columns)
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
		auto arg = columns_find(columns, &name->string);
		if (arg)
			stmt_error(self, name, "argument redefined");

		// add argument
		arg = column_allocate();
		column_set_name(arg, &name->string);
		encode_null(&arg->constraints.value);
		columns_add(columns, arg);

		// type
		int  type_size;
		auto type = parse_type(self->lex, &type_size);
		column_set_type(arg, type, type_size);

		// ,
		if (! stmt_if(self, ','))
			break;
	}

	// )
	stmt_expect(self, ')');
}

void
parse_function_create(Stmt* self, bool or_replace)
{
	// CREATE [OR REPLACE] FUNCTION [schema.]name (args)
	// [RETURN type [(args)]]
	// BEGIN
	//   block
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
		stmt_error(self, NULL, "name expected");
	udf_config_set_schema(stmt->config, &schema);
	udf_config_set_name(stmt->config, &name);

	// ensure schema exists and not system
	auto schema_obj =
		schema_mgr_find(&share()->db->catalog.schema_mgr,
		                &schema, true);
	if (! schema_obj->config->create)
		error("system schema '%.*s' cannot be used to create objects",
		      str_size(&schema), str_of(&schema));

	// (args)
	parse_function_args(self, &stmt->config->args);

	// [RETURN]
	auto ret = stmt_if(self, KRETURN);
	if (ret)
	{
		// table | type
		auto ast = stmt_next_shadow(self);
		if (ast->id != KNAME)
			stmt_error(self, ast, "unrecognized data type");

		int type;
		if (str_is_case(&ast->string, "table", 5))
		{
			type = TYPE_STORE;
		} else
		{
			stmt_push(self, ast);
			int type_size;
			type = parse_type(self->lex, &type_size);
		}

		udf_config_set_type(stmt->config, type);

		// [(args)]
		if (type == TYPE_STORE)
		{
			parse_function_args(self, &stmt->config->returning);
			if (! stmt->config->returning.count)
				stmt_error(self, NULL, "invalid number of returning columns");
		}
	}

	// create new namespace
	auto parser = self->parser;
	auto ns     = namespaces_add(&parser->nss, self->block->ns, stmt->config);

	// precreate arguments as variables
	list_foreach(&stmt->config->args.list)
	{
		auto column = list_at(Column, link);
		vars_add(&ns->vars, &column->name, column->type, true);
	}

	// BEGIN
	auto begin = stmt_expect(self, KBEGIN);

	auto block = blocks_add(&ns->blocks, self->block, self);
	parse_block(parser, block);

	// END
	auto end = stmt_expect(self, KEND);

	// set function text
	Str text;
	str_init(&text);
	str_set(&text, parser->lex.start + begin->pos_start,
	        end->pos_end - begin->pos_start);
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
		stmt_error(self, NULL, "name expected");
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
		stmt_error(self, NULL, "name expected");

	// RENAME
	stmt_expect(self, KRENAME);

	// [TO]
	stmt_if(self, KTO);

	// name
	if (! parse_target(self, &stmt->schema_new, &stmt->name_new))
		stmt_error(self, NULL, "name expected");
}
