
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
#include <sonata_auth.h>
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
parse_checkpoint(Stmt* self)
{
	// CHECKPOINT [WORKERS n]
	auto stmt = ast_checkpoint_allocate();
	self->ast = &stmt->ast;

	// [WORKERS]
	if (stmt_if(self, KWORKERS))
	{
		stmt->workers = stmt_if(self, KINT);
		if (! stmt->workers)
			error("CHECKPOINT WORKERS <int> expected");
		if (stmt->workers->integer <= 0)
			error("CHECKPOINT WORKERS number must be positive");
	}
}
