
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

void
parse_publish(Stmt* self)
{
	// PUBLISH INTO [user.]topic [value, ...]
	auto stmt = ast_publish_allocate(self->block);
	self->ast = &stmt->ast;

	// INTO
	stmt_expect(self, KINTO);

	// [user.]name
	Str user;
	Str name;
	auto path = parse_target(self, &user, &name);

	// find topic
	auto topic = catalog_find_topic(&share()->db->catalog, &user, &name, false);
	if (! topic)
		stmt_error(self, path, "topic not found");
	stmt->topic = topic;

	access_add(&self->parser->program->access, &topic->rel,
	           LOCK_SHARED, PERM_PUBLISH);

	// [value, ...]
	if (stmt_if(self, KEOF) || stmt_if(self, ';'))
		return;

	// prepare values
	auto parser = self->parser;
	stmt->values = set_cache_create(parser->set_cache, &parser->program->sets);
	set_prepare(stmt->values, 1, 0, NULL);

	// topics has single json column
	auto column = columns_first(&share()->db->catalog.topic_columns);
	for (;;)
	{
		// prepare row
		auto row = set_reserve(stmt->values);

		// parse and set argument value
		auto value = &row[0];
		parse_value(self, NULL, column, value);

		// ,
		if (! stmt_if(self, ','))
			break;
	}
}
