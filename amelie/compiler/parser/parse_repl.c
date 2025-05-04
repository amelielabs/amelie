
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
parse_repl_start(Scope* self)
{
	// START REPL
	auto stmt = ast_repl_ctl_allocate(true);
	self->stmt->ast = &stmt->ast;
}

void
parse_repl_stop(Scope* self)
{
	// STOP REPL
	auto stmt = ast_repl_ctl_allocate(false);
	self->stmt->ast = &stmt->ast;
}

void
parse_repl_subscribe(Scope* self)
{
	// SUBSCRIBE id
	auto stmt = ast_repl_subscribe_allocate();
	self->stmt->ast = &stmt->ast;
	stmt->id  = scope_expect(self, KSTRING);
}

void
parse_repl_unsubscribe(Scope* self)
{
	// UNSUBSCRIBE
	auto stmt = ast_repl_subscribe_allocate();
	self->stmt->ast = &stmt->ast;
	stmt->id  = NULL;
}
