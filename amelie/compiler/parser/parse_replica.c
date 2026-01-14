
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
#include <amelie_storage>
#include <amelie_repl>
#include <amelie_vm>
#include <amelie_parser.h>

void
parse_replica_create(Stmt* self)
{
	// CREATE REPLICA [IF NOT EXISTS] id uri
	auto stmt = ast_replica_create_allocate();
	self->ast = &stmt->ast;
	stmt->config = replica_config_allocate();

	// if not exists
	stmt->if_not_exists = parse_if_not_exists(self);

	// id
	auto id = stmt_expect(self, KSTRING);
	Uuid uuid;
	uuid_init(&uuid);
	uuid_set(&uuid, &id->string);
	replica_config_set_id(stmt->config, &uuid);

	// uri
	auto uri = stmt_expect(self, KSTRING);
	uri_parse(&stmt->config->endpoint, &uri->string);
	opt_string_set_raw(&stmt->config->endpoint.service, "repl", 4);
}

void
parse_replica_drop(Stmt* self)
{
	// DROP REPLICA [IF EXISTS] id
	auto stmt = ast_replica_drop_allocate();
	self->ast = &stmt->ast;

	// if exists
	stmt->if_exists = parse_if_exists(self);

	// id
	stmt->id = stmt_expect(self, KSTRING);
}
