
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
#include <amelie_engine>
#include <amelie_storage>
#include <amelie_repl>
#include <amelie_vm>
#include <amelie_parser.h>

hot void
parse_delete(Stmt* self)
{
	// DELETE FROM name
	// [WHERE expr]
	// [RETURNING expr]
	auto stmt = ast_delete_allocate(self->block);
	self->ast = &stmt->ast;
	self->ret = &stmt->ret;

	// FROM
	auto from = stmt_expect(self, KFROM);

	// table
	parse_from(self, &stmt->from, ACCESS_RW, false);
	if (from_empty(&stmt->from) || from_is_join(&stmt->from))
		stmt_error(self, from, "table name expected");
	auto target = from_first(&stmt->from);
	if (! target_is_table(target))
		stmt_error(self, from, "table name expected");
	stmt->table = target->from_table;

	// ensure primary index is used
	if (target->from_index)
		if (table_primary(stmt->table) != target->from_index)
			stmt_error(self, from, "DELETE supports only primary index");

	// [WHERE]
	if (stmt_if(self, KWHERE))
	{
		Expr ctx;
		expr_init(&ctx);
		ctx.select = true;
		ctx.names  = &stmt->from.list_names;
		ctx.from   = &stmt->from;
		stmt->expr_where = parse_expr(self, &ctx);
	}

	// [RETURNING]
	if (stmt_if(self, KRETURNING))
	{
		parse_returning(&stmt->ret, self, NULL);
		parse_returning_resolve(&stmt->ret, self, &stmt->from);
	}
}
