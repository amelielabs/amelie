
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
#include <amelie_aggr.h>
#include <amelie_executor.h>
#include <amelie_vm.h>
#include <amelie_parser.h>

void
parse_execute(Stmt* self)
{
	// EXECUTE [schema.]name [(args)]
	auto stmt = ast_execute_allocate();
	self->ast = &stmt->ast;

	// name
	Str schema;
	Str name;
	if (! parse_target(self, &schema, &name))
		error("EXECUTE <name> expected");

	// find function
	stmt->udf = udf_mgr_find(&self->db->udf_mgr, &schema, &name, true);

	// todo: parse arguments
}
