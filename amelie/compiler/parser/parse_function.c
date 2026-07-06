
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
		int  size_flat;
		int  size;
		auto type = parse_type(self->lex, &size, &size_flat);
		column_set_type(arg, type, size);

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
	// [DESCRIPTION text]
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
	udf_config_set_user(stmt->config, self->parser->user);
	udf_config_set_name(stmt->config, &name->string);

	// copy existing grants
	if (stmt->or_replace)
	{
		auto udf = catalog_find_udf(&share()->db->catalog, self->parser->user,
		                            &name->string, false);
		if (udf)
			grants_copy(&stmt->config->grants, &udf->config->grants);
	}

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
			int type_size_flat;
			int type_size;
			type = parse_type(self->lex, &type_size, &type_size_flat);
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

	// [DESCRIPTION]
	auto description = stmt_if(self, KDESCRIPTION);
	if (description)
	{
		auto text = stmt_expect(self, KSTRING);
		udf_config_set_description(stmt->config, &text->string);
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
	// DROP FUNCTION [IF EXISTS] name [CASCADE]
	auto stmt = ast_function_drop_allocate();
	self->ast = &stmt->ast;

	// if exists
	stmt->if_exists = parse_if_exists(self);

	// name
	auto name  = stmt_expect(self, KNAME);
	stmt->name = name->string;

	// [CASCADE]
	stmt->cascade = stmt_if(self, KCASCADE) != NULL;
}

void
parse_function_alter(Stmt* self)
{
	// ALTER FUNCTION [IF EXISTS] name RENAME name
	// ALTER FUNCTION [IF EXISTS] name DESCRIPTION text
	auto stmt = ast_function_alter_allocate();
	self->ast = &stmt->ast;

	// if exists
	stmt->if_exists = parse_if_exists(self);

	// name
	auto name  = stmt_expect(self, KNAME);
	stmt->name = name->string;

	// RENAME
	if (stmt_if(self, KRENAME))
	{
		// TO
		stmt_expect(self, KTO);
		stmt->type = FUNCTION_ALTER_RENAME;

		// name
		name = stmt_expect(self, KNAME);
		stmt->name_new = name->string;
		return;
	}

	// DESCRIPTION
	if (stmt_if(self, KDESCRIPTION))
	{
		auto text = stmt_expect(self, KSTRING);
		stmt->type = FUNCTION_ALTER_DESCRIPTION;
		stmt->description = text->string;
		return;
	}

	stmt_error(self, NULL, "RENAME or DESCRIPTION expected");
}
