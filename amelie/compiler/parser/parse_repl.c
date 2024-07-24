
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

void
parse_repl_start(Stmt* self)
{
	// START REPL
	auto stmt = ast_repl_ctl_allocate(true);
	self->ast = &stmt->ast;
}

void
parse_repl_stop(Stmt* self)
{
	// STOP REPL
	auto stmt = ast_repl_ctl_allocate(false);
	self->ast = &stmt->ast;
}

void
parse_repl_promote(Stmt* self)
{
	// PROMOTE id | RESET
	auto stmt = ast_repl_promote_allocate();
	self->ast = &stmt->ast;

	stmt->id = stmt_if(self, KSTRING);
	if (stmt->id == NULL)
	{
		if (! stmt_if(self, KRESET))
			error("PROMOTE id | RESET expected");
	}
}
