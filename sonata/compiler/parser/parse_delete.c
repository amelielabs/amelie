
//
// sonata.
//
// SQL Database for JSON.
//

#include <sonata_runtime.h>
#include <sonata_io.h>
#include <sonata_lib.h>
#include <sonata_data.h>
#include <sonata_config.h>
#include <sonata_auth.h>
#include <sonata_http.h>
#include <sonata_client.h>
#include <sonata_server.h>
#include <sonata_def.h>
#include <sonata_transaction.h>
#include <sonata_index.h>
#include <sonata_storage.h>
#include <sonata_wal.h>
#include <sonata_db.h>
#include <sonata_value.h>
#include <sonata_aggr.h>
#include <sonata_executor.h>
#include <sonata_vm.h>
#include <sonata_parser.h>

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
