
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
#include <sonata_semantic.h>
#include <sonata_compiler.h>
#include <sonata_backup.h>
#include <sonata_node.h>
#include <sonata_repl.h>

void
repl_init(Repl* self, Db* db, NodeMgr* node_mgr)
{
	self->role     = REPL_PRIMARY;
	self->node_mgr = node_mgr;
	self->db       = db;
	pusher_mgr_init(&self->pusher_mgr, &db->wal);
}

void
repl_free(Repl* self)
{
	unused(self);
}

void
repl_open(Repl* self)
{
	// set role
	self->role = repl_role_read(&config()->repl_role.string);

	// set to read-only, even if replication is not enabled
	if (self->role == REPL_REPLICA)
		var_int_set(&config()->read_only, true);
}

void
repl_start(Repl* self)
{
	if (var_int_of(&config()->repl))
		return;

	log("replication: start as '%s'", repl_role_of(self->role));
	var_int_set(&config()->repl, true);

	switch (self->role) {
	case REPL_PRIMARY:
	{
		// set system as read-write
		var_int_set(&config()->read_only, false);

		// create pusher for each replica node
		pusher_mgr_start(&self->pusher_mgr, self->node_mgr);
		break;
	}
	case REPL_REPLICA:
	{
		auto primary = &config()->repl_primary.string;
		if (str_empty(primary))
			error("replication: primary node is not defined");

		auto node = node_mgr_find(self->node_mgr, primary);
		if (unlikely(! node))
			error("replication: unknown primary node <%.*s>",
			      str_size(primary), str_of(primary));

		// set system as read-only
		var_int_set(&config()->read_only, true);
		break;
	}
	}
}

void
repl_stop(Repl* self)
{
	if (! var_int_of(&config()->repl))
		return;

	log("replication: stop");
	var_int_set(&config()->repl, false);

	switch (self->role) {
	case REPL_PRIMARY:
		// stop pushers
		pusher_mgr_stop(&self->pusher_mgr);
		// todo: removes wal snapshot?
		break;
	case REPL_REPLICA:
		break;
	}
}

void
repl_promote(Repl* self, Str* role, Str* primary)
{
	// set role
	int role_id = repl_role_read(role);
	if (role_id == -1)
		error("replication: unknown role '%.*s'", str_size(role), str_of(role));

	if (role_id == REPL_REPLICA && str_empty(primary))
		error("%s", "replication: primary node is not specified");

	auto node = node_mgr_find(self->node_mgr, primary);
	if (unlikely(! node))
		error("replication: unknown primary node <%.*s>",
			  str_size(primary), str_of(primary));

	// stop
	repl_stop(self);

	// apply configuration
	var_string_set(&config()->repl_role, role);
	var_string_set(&config()->repl_primary, primary);
	self->role = role_id;

	// start
	repl_start(self);
}
