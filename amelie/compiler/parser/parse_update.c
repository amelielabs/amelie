
//
// amelie.
//
// Real-Time SQL Database.
//

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_data.h>
#include <amelie_config.h>
#include <amelie_user.h>
#include <amelie_http.h>
#include <amelie_client.h>
#include <amelie_server.h>
#include <amelie_row.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_value.h>
#include <amelie_aggr.h>
#include <amelie_executor.h>
#include <amelie_vm.h>
#include <amelie_parser.h>

hot Ast*
parse_update_expr(Stmt* self)
{
	// SET
	if (! stmt_if(self, KSET))
		error("UPDATE <SET> expected");

	// path = expr [, ... ]
	Ast* expr_prev = NULL;
	Ast* expr = NULL;
	for (;;)
	{
		auto op = ast(KSET);

		// name or path
		op->l = stmt_next(self);
		switch (op->l->id) {
		case KNAME:
		case KNAME_COMPOUND:
			break;
		default:
			error("UPDATE name SET <path> expected");
			break;
		}

		// =
		if (! stmt_if(self, '='))
			error("UPDATE name SET path <=> expected");

		// expr
		op->r = parse_expr(self, NULL);

		// op(path, expr)
		if (expr == NULL)
			expr = op;
		else
			expr_prev->next = op;
		expr_prev = op;

		// ,
		if(! stmt_if(self, ','))
			break;
	}

	return expr;
}

hot void
parse_update(Stmt* self)
{
	// UPDATE name SET path = expr [, ... ] [WHERE expr]
	auto stmt = ast_update_allocate();
	self->ast = &stmt->ast;

	// table_name, expression or join
	int level = target_list_next_level(&self->target_list);
	stmt->target = parse_from(self, level);
	stmt->table = stmt->target->table;
	if (stmt->table == NULL)
		error("UPDATE <table name> expected");
	if (stmt->target->next_join)
		error("UPDATE JOIN is not supported");

	// todo: ensure target uses primary index

	// SET path = expr [, ... ]
	stmt->expr_update = parse_update_expr(self);

	// [WHERE]
	if (stmt_if(self, KWHERE))
		stmt->expr_where = parse_expr(self, NULL);

	// [RETURNING]
	if (stmt_if(self, KRETURNING))
		stmt->returning = parse_expr(self, NULL);
}
