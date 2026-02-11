
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
	// CREATE [OR REPLACE] FUNCTION name (args)
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
	auto name = stmt_expect(self, KNAME);
	udf_config_set_db(stmt->config, self->parser->db);
	udf_config_set_name(stmt->config, &name->string);

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
	auto name  = stmt_expect(self, KNAME);
	stmt->name = name->string;
}

void
parse_function_alter(Stmt* self)
{
	// ALTER FUNCTION [IF EXISTS] name RENAME name
	auto stmt = ast_function_alter_allocate();
	self->ast = &stmt->ast;

	// if exists
	stmt->if_exists = parse_if_exists(self);

	// name
	auto name  = stmt_expect(self, KNAME);
	stmt->name = name->string;

	// RENAME
	stmt_expect(self, KRENAME);

	// [TO]
	stmt_if(self, KTO);

	// name
	auto name_new  = stmt_expect(self, KNAME);
	stmt->name_new = name_new->string;
}
