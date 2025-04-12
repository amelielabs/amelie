
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

hot void
parse_delete(Stmt* self)
{
	// DELETE FROM name
	// [WHERE expr]
	// [RETURNING expr [FORMAT name]]
	auto stmt = ast_delete_allocate();
	self->ast = &stmt->ast;

	// FROM
	auto from = stmt_expect(self, KFROM);

	// table
	parse_from(self, &stmt->targets, ACCESS_RW, false);
	if (targets_empty(&stmt->targets) || targets_is_join(&stmt->targets))
		stmt_error(self, from, "table name expected");
	auto target = targets_outer(&stmt->targets);
	if (! target_is_table(target))
		stmt_error(self, from, "table name expected");
	stmt->table = target->from_table;

	// ensure primary index is used
	if (target->from_index)
		if (table_primary(stmt->table) != target->from_index)
			stmt_error(self, from, "DELETE supports only primary index");

	// prevent from using heap
	if (target->from_heap)
		stmt_error(self, from, "DELETE supports only primary index");

	// [WHERE]
	if (stmt_if(self, KWHERE))
	{
		Expr ctx;
		expr_init(&ctx);
		ctx.select  = true;
		ctx.targets = &stmt->targets;
		stmt->expr_where = parse_expr(self, &ctx);
	}

	// [RETURNING]
	if (stmt_if(self, KRETURNING))
	{
		parse_returning(&stmt->ret, self, NULL);
		parse_returning_resolve(&stmt->ret, self, &stmt->targets);
	}
}
