
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
parse_token_create(Stmt* self)
{
	// CREATE TOKEN name [EXPIRE interval]
	auto stmt = ast_token_create_allocate();
	self->ast = &stmt->ast;
	self->ret = &stmt->ret;

	// name
	stmt->user = stmt_expect(self, KNAME);

	// EXPIRE
	if (stmt_if(self, KEXPIRE))
	{
		// value
		stmt->expire = stmt_expect(self, KSTRING);
	}

	// set returning column
	auto column = column_allocate();
	column_set_name(column, &stmt->user->string);
	column_set_type(column, TYPE_JSON, -1);
	columns_add(&stmt->ret.columns, column);
}
