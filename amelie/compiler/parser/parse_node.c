
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
parse_node_create(Stmt* self)
{
	// CREATE NODE [IF NOT EXISTS] [id] FOR COMPUTE
	auto stmt = ast_node_create_allocate();
	self->ast = &stmt->ast;

	// if not exists
	stmt->if_not_exists = parse_if_not_exists(self);

	// id
	stmt->id = stmt_if(self, KSTRING);

	// FOR COMPUTE
	if (! stmt_if(self, KFOR))
		error("CREATE NODE <FOR> expected");
	if (! stmt_if(self, KCOMPUTE))
		error("CREATE NODE FOR <COMPUTE> expected");
}

void
parse_node_drop(Stmt* self)
{
	// DROP NODE [IF EXISTS] id
	auto stmt = ast_node_drop_allocate();
	self->ast = &stmt->ast;

	// if exists
	stmt->if_exists = parse_if_exists(self);

	// id
	stmt->id = stmt_if(self, KSTRING);
	if (! stmt->id)
		error("DROP NODE <id> expected");
}
