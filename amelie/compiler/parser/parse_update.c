
//
// sonata.
//
// Real-Time SQL Database.
//

#include <sonata_runtime.h>
#include <sonata_io.h>
#include <sonata_lib.h>
#include <sonata_data.h>
#include <sonata_config.h>
#include <sonata_user.h>
#include <sonata_http.h>
#include <sonata_client.h>
#include <sonata_server.h>
#include <sonata_row.h>
#include <sonata_transaction.h>
#include <sonata_index.h>
#include <sonata_partition.h>
#include <sonata_wal.h>
#include <sonata_db.h>
#include <sonata_value.h>
#include <sonata_aggr.h>
#include <sonata_executor.h>
#include <sonata_vm.h>
#include <sonata_parser.h>

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
