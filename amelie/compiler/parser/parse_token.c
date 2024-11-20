
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
#include <amelie_value.h>
#include <amelie_store.h>
#include <amelie_user.h>
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
#include <amelie_executor.h>
#include <amelie_vm.h>
#include <amelie_parser.h>

void
parse_token_create(Stmt* self)
{
	// CREATE TOKEN name [EXPIRE interval]
	auto stmt = ast_token_create_allocate();
	self->ast = &stmt->ast;

	// name
	stmt->user = stmt_if(self, KNAME);
	if (! stmt->user)
		error("CREATE TOKEN <name> expected");

	// EXPIRE
	if (stmt_if(self, KEXPIRE))
	{
		// value
		stmt->expire = stmt_if(self, KSTRING);
		if (! stmt->expire)
			error("CREATE TOKEN EXPIRE <value> string expected");
	}
}
