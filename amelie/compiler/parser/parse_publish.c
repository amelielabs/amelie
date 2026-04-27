
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
	// PUBLISH TO [user.]topic [expr]
	auto stmt = ast_publish_allocate(self->block);
	self->ast = &stmt->ast;

	// TO
	stmt_expect(self, KTO);

	// [user.]name
	Str user;
	Str name;
	auto path = parse_target(self, &user, &name);

	// find topic
	auto topic = topic_mgr_find(&share()->db->catalog.topic_mgr, &user, &name, false);
	if (! topic)
		stmt_error(self, path, "topic not found");
	stmt->topic = topic;

	access_add(&self->parser->program->access, &topic->rel,
	           LOCK_SHARED, PERM_PUBLISH);

	// [expr]
	if (!stmt_if(self, KEOF) && !stmt_if(self, ';'))
		stmt->expr = parse_expr(self, NULL);
}
