
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
#include <sonata_node.h>
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
	stmt->config = node_config_allocate();

	// if not exists
	stmt->if_not_exists = parse_if_not_exists(self);

	// id
	auto id = stmt_if(self, KSTRING);
	if (id)
	{
		Uuid uuid;
		uuid_from_string(&uuid, &id->string);
		node_config_set_id(stmt->config, &uuid);
	} else
	{
		Uuid uuid;
		uuid_generate(&uuid, global()->random);
		node_config_set_id(stmt->config, &uuid);
	}

	// URI uri
	if (stmt_if(self, KURI))
	{
		auto uri = stmt_if(self, KSTRING);
		if (! uri)
			error("CREATE NODE URI <string> expected");
		node_config_set_uri(stmt->config, &uri->string);
	}

	// [FOR COMPUTE|REPL]
	node_config_set_type(stmt->config, NODE_COMPUTE);
	if (stmt_if(self, KFOR))
	{
		if (stmt_if(self, KCOMPUTE))
			node_config_set_type(stmt->config, NODE_COMPUTE);
		else
		if (stmt_if(self, KREPL) ||
		    stmt_if(self, KREPLICATION))
			node_config_set_type(stmt->config, NODE_REPL);
		else
			error("CREATE NODE FOR <COMPUTE | REPLICATION> expected");
	}

	if (stmt->config->type == NODE_REPL && str_empty(&stmt->config->uri))
		error("CREATE NODE <URI> is missing for replication node");
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
	// ALTER NODE id URI uri
	auto stmt = ast_node_alter_allocate();
	self->ast = &stmt->ast;

	stmt->config = node_config_allocate();

	// id
	auto id = stmt_if(self, KSTRING);
	if (! id)
		error("ALTER NODE <id> expected");
	Uuid uuid;
	uuid_from_string(&uuid, &id->string);
	node_config_set_id(stmt->config, &uuid);

	// URI uri
	if (! stmt_if(self, KURI))
		error("ALTER NODE <URI> expected");
	auto uri = stmt_if(self, KSTRING);
	if (! uri)
		error("ALTER NODE URI <string> expected");
	node_config_set_uri(stmt->config, &uri->string);
}
