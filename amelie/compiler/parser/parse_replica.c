
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

void
parse_replica_create(Stmt* self)
{
	// CREATE REPLICA [IF NOT EXISTS] id uri
	auto stmt = ast_replica_create_allocate();
	self->ast = &stmt->ast;

	// if not exists
	stmt->if_not_exists = parse_if_not_exists(self);

	// id
	stmt->id = stmt_expect(self, KSTRING);

	// options
	for (;;)
	{
		// name
		auto name = stmt_next_shadow(self);
		if (name->id == KEOF)
			break;
		if (name->id != KNAME)
			stmt_error(self, name, "option name expected");

		// [=]
		stmt_if(self, '=');

		// string
		auto value = stmt_expect(self, KSTRING);
		if (str_is_case(&name->string, "uri", 3))
			remote_set(&stmt->remote, REMOTE_URI, &value->string);
		else
		if (str_is_case(&name->string, "tls_ca", 6))
			remote_set(&stmt->remote, REMOTE_FILE_CA, &value->string);
		else
		if (str_is_case(&name->string, "tls_capath", 10))
			remote_set(&stmt->remote, REMOTE_PATH_CA, &value->string);
		else
		if (str_is_case(&name->string, "tls_cert", 8))
			remote_set(&stmt->remote, REMOTE_FILE_CERT, &value->string);
		else
		if (str_is_case(&name->string, "tls_key", 7))
			remote_set(&stmt->remote, REMOTE_FILE_KEY, &value->string);
		else
		if (str_is_case(&name->string, "token", 5))
			remote_set(&stmt->remote, REMOTE_TOKEN, &value->string);
		else
			stmt_error(self, name, "unrecognized option");
	}

	// validate options
	if (str_empty(remote_get(&stmt->remote, REMOTE_URI)))
		stmt_error(self, NULL, "URI is not defined");
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
