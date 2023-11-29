
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
#include <monotone_index.h>
#include <monotone_storage.h>
#include <monotone_wal.h>
#include <monotone_db.h>
#include <monotone_value.h>
#include <monotone_aggr.h>
#include <monotone_request.h>
#include <monotone_vm.h>
#include <monotone_parser.h>

hot void
parse_delete(Stmt* self)
{
	// DELETE FROM name [WHERE expr]
	auto stmt = ast_delete_allocate();
	self->ast = &stmt->ast;

	// FROM
	if (! stmt_if(self, KFROM))
		error("DELETE <FROM> expected");

	// table_name, expression or join
	int level = target_list_next_level(&self->target_list);
	stmt->target = parse_from(self, level);
	stmt->table = stmt->target->table;
	if (stmt->table == NULL)
		error("DELETE FROM <table name> expected");

	// todo: check primary index

	// [WHERE]
	if (stmt_if(self, KWHERE))
		stmt->expr_where = parse_expr(self, NULL);

	// combine join on and where expression
	stmt->expr_where =
		parse_from_join_on_and_where(stmt->target, stmt->expr_where);
}
