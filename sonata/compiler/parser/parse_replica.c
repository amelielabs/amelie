
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
#include <sonata_def.h>
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

void
parse_replica_create(Stmt* self)
{
	// CREATE REPLICA [IF NOT EXISTS] id uri
	auto stmt = ast_replica_create_allocate();
	self->ast = &stmt->ast;

	// if not exists
	stmt->if_not_exists = parse_if_not_exists(self);

	// id
	stmt->id = stmt_if(self, KSTRING);
	if (! stmt->id)
		error("CREATE REPLICA <id> expected");

	// uri
	stmt->uri = stmt_if(self, KSTRING);
	if (! stmt->uri)
		error("CREATE REPLICA id <uri> expected");
}

void
parse_replica_drop(Stmt* self)
{
	// DROP REPLICA [IF EXISTS] id
	auto stmt = ast_replica_drop_allocate();
	self->ast = &stmt->ast;

	// if exists
	stmt->if_exists = parse_if_exists(self);

	// id
	stmt->id = stmt_if(self, KSTRING);
	if (! stmt->id)
		error("DROP REPLICA <id> expected");
}
