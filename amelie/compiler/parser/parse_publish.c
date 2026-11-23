
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
#include <amelie_heap>
#include <amelie_storage>
#include <amelie_repl>
#include <amelie_vm>
#include <amelie_parser.h>

void
parse_publish(Stmt* self)
{
	// PUBLISH db.relation [value, ...]
	auto stmt = ast_publish_allocate(self->block);
	self->ast = &stmt->ast;

	// [INTO]
	stmt_if(self, KINTO);

	// db.name
	auto path = stmt_expect(self, KNAME_COMPOUND);
	Str name_db;
	Str name = path->string;
	str_init(&name_db);
	str_split(&path->string, &name_db, '.');
	str_advance(&name, str_size(&name_db) + 1);
	if (str_chr(&name, '.'))
		stmt_error(self, path, "invalid target");

	stmt->db  = name_db;
	stmt->rel = name;

	// mark for rw
	access_set_publish(&self->parser->program->access);

	// [value, ...]
	if (stmt_if(self, KEOF) || stmt_if(self, ';'))
		return;

	// prepare values
	auto parser = self->parser;
	stmt->values = set_cache_create(parser->set_cache, &parser->program->sets);
	set_prepare(stmt->values, 1, 0, NULL);

	// json column
	auto column = columns_first(&share()->storage->publish);
	for (;;)
	{
		// prepare row
		auto row = set_reserve(stmt->values);

		// parse and set argument value
		auto value = &row[0];
		parse_value(self, &stmt->from, column, value);

		// ,
		if (! stmt_if(self, ','))
			break;
	}

	// check values limit
	if (! local_limit(self->parser->local, LIMIT_VALUES, stmt->values->count_rows))
		stmt_error(self, NULL, "values limit reached");
}
