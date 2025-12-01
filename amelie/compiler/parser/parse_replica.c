
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
#include <amelie_output.h>
#include <amelie_executor.h>
#include <amelie_func.h>
#include <amelie_vm.h>
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
