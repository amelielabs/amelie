
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

hot void
parse_delete(Stmt* self)
{
	// DELETE FROM name [WHERE expr] [RETURNING expr]
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
	if (stmt->target->next_join)
		error("DELETE FROM JOIN is not supported");

	// todo: check primary index

	// [WHERE]
	if (stmt_if(self, KWHERE))
		stmt->expr_where = parse_expr(self, NULL);

	// [RETURNING]
	if (stmt_if(self, KRETURNING))
		stmt->returning = parse_expr(self, NULL);
}
