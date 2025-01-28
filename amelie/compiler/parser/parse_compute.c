
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
parse_compute_alter(Stmt* self)
{
	// ALTER COMPUTE [POOL] ADD/DROP [NODE] [ID]
	auto stmt = ast_compute_alter_allocate();
	self->ast = &stmt->ast;

	// [POOL]
	stmt_if(self, KPOOL);

	// ADD / DROP
	if (stmt_if(self, KADD))
		stmt->add = true;
	else
	if (stmt_if(self, KDROP))
		stmt->add = false;
	else
		stmt_error(self, NULL, "ADD or DROP expected");

	// [NODE]
	stmt_if(self, KNODE);

	// [id]
	stmt->id = stmt_if(self, KSTRING);

	if (!stmt->add && !stmt->id)
		stmt_error(self, NULL, "id is expected for the DROP command");
}
