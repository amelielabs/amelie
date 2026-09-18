
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

void
parse_api_create(Stmt* self)
{
	// CREATE URI 'uri' ON [user.]name [DESCRIPTION]
	auto stmt = ast_api_create_allocate();
	self->ast = &stmt->ast;

	// create api config
	auto config = api_allocate();
	stmt->config = config;

	// uri
	auto uri = stmt_expect(self, KSTRING);
	api_set_uri(config, &uri->string);

	parse_api_create_inline(self, config);
}

void
parse_api_create_inline(Stmt* self, Api* config)
{
	// ON [user.]name [DESCRIPTION]
	stmt_expect(self, KON);

	// [user.]name
	Str user;
	Str name;
	parse_target(self, &user, &name);
	api_set_rel_user(config, &user);
	api_set_rel(config, &name);

	// set options
	for (;;)
	{
		// name value
		auto name = stmt_next_shadow(self);
		if (name->id != KNAME)
		{
			stmt_push(self, name);
			break;
		}

		// DESCRIPTION string
		if (str_is_case(&name->string, "description", 11))
		{
			auto text = stmt_expect(self, KSTRING);
			api_set_description(config, &text->string);
			continue;
		}

		stmt_error(self, name, "unrecognized option");
	}
}

void
parse_api_drop(Stmt* self)
{
	// DROP API 'uri';
	auto stmt = ast_api_drop_allocate();
	self->ast = &stmt->ast;

	// uri
	auto uri = stmt_expect(self, KSTRING);
	stmt->uri = uri->string;
}
