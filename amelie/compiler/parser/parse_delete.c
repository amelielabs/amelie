
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
#include <amelie_data.h>
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
#include <amelie_value.h>
#include <amelie_store.h>
#include <amelie_executor.h>
#include <amelie_vm.h>
#include <amelie_parser.h>

hot void
parse_delete(Stmt* self)
{
	// DELETE FROM name
	// [WHERE expr]
	// [RETURNING expr]
	auto stmt = ast_delete_allocate();
	self->ast = &stmt->ast;

	// FROM
	if (! stmt_if(self, KFROM))
		error("DELETE <FROM> expected");

	// table_name, expression or join
	int level = target_list_next_level(&self->target_list);
	stmt->target = parse_from(self, level);
	stmt->table = stmt->target->from_table;
	if (stmt->table == NULL)
		error("DELETE FROM <table name> expected");
	if (stmt->target->next_join)
		error("DELETE FROM JOIN is not supported");
	if (stmt->target->from_table_index)
		if (table_primary(stmt->table) != stmt->target->from_table_index)
			error("DELETE only primary index supported");

	// [WHERE]
	if (stmt_if(self, KWHERE))
		stmt->expr_where = parse_expr(self, NULL);

	// [RETURNING]
	if (stmt_if(self, KRETURNING))
		stmt->returning = parse_expr(self, NULL);
}
