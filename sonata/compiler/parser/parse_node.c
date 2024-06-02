
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
parse_node_create(Stmt* self)
{
	// CREATE NODE [IF NOT EXISTS] [id] [URI uri] [FOR COMPUTE|REPL]
	auto stmt = ast_node_create_allocate();
	self->ast = &stmt->ast;

	// if not exists
	stmt->if_not_exists = parse_if_not_exists(self);

	// id
	stmt->id = stmt_if(self, KSTRING);

	// URI uri
	if (stmt_if(self, KURI))
	{
		stmt->uri = stmt_if(self, KSTRING);
		if (! stmt->uri)
			error("CREATE NODE URI <string> expected");
	}

	// [FOR COMPUTE|REPL]
	if (stmt_if(self, KFOR))
	{
		stmt->type = stmt_next(self);
		switch (stmt->type->id) {
		case KCOMPUTE:
		case KREPL:
		case KREPLICATION:
			break;
		default:
			error("CREATE NODE FOR <COMPUTE | REPLICATION> expected");
			break;
		}
	}
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

void
parse_node_alter(Stmt* self)
{
	// ALTER NODE [IF EXISTS] id URI uri
	auto stmt = ast_node_alter_allocate();
	self->ast = &stmt->ast;

	// if exists
	stmt->if_exists = parse_if_exists(self);

	// id
	stmt->id = stmt_if(self, KSTRING);
	if (! stmt->id)
		error("ALTER NODE <id> expected");

	// URI uri
	if (! stmt_if(self, KURI))
		error("ALTER NODE <URI> expected");
	stmt->uri = stmt_if(self, KSTRING);
	if (! stmt->uri)
		error("ALTER NODE URI <string> expected");
}
