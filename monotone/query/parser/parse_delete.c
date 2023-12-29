
//
// indigo
//
// SQL OLTP database
//

#include <indigo_runtime.h>
#include <indigo_io.h>
#include <indigo_data.h>
#include <indigo_lib.h>
#include <indigo_config.h>
#include <indigo_auth.h>
#include <indigo_def.h>
#include <indigo_transaction.h>
#include <indigo_index.h>
#include <indigo_storage.h>
#include <indigo_wal.h>
#include <indigo_db.h>
#include <indigo_value.h>
#include <indigo_aggr.h>
#include <indigo_request.h>
#include <indigo_vm.h>
#include <indigo_parser.h>

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
