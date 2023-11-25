
//
// monotone
//
// SQL OLTP database
//

#include <monotone_runtime.h>
#include <monotone_io.h>
#include <monotone_data.h>
#include <monotone_lib.h>
#include <monotone_config.h>
#include <monotone_auth.h>
#include <monotone_def.h>
#include <monotone_transaction.h>
#include <monotone_snapshot.h>
#include <monotone_storage.h>
#include <monotone_wal.h>
#include <monotone_db.h>
#include <monotone_value.h>
#include <monotone_aggr.h>
#include <monotone_request.h>
#include <monotone_vm.h>
#include <monotone_parser.h>

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

	// todo: ensure target uses primary index

	// SET path = expr [, ... ]
	stmt->expr_update = parse_update_expr(self);

	// [WHERE]
	if (stmt_if(self, KWHERE))
		stmt->expr_where = parse_expr(self, NULL);

	// combine join on and where expression
	stmt->expr_where =
		parse_from_join_on_and_where(stmt->target, stmt->expr_where);
}
