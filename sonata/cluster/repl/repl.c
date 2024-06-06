
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
#include <sonata_semantic.h>
#include <sonata_compiler.h>
#include <sonata_backup.h>
#include <sonata_repl.h>

static inline char*
repl_role_of(ReplRole role)
{
	switch (role) {
	case REPL_PRIMARY: return "primary";
	case REPL_REPLICA: return "replica";
	}
	return NULL;
}

void
repl_init(Repl* self, Db* db)
{
	self->role = REPL_PRIMARY;
	replica_mgr_init(&self->replica_mgr, db);
}

void
repl_free(Repl* self)
{
	replica_mgr_free(&self->replica_mgr);
}

void
repl_open(Repl* self)
{
	// restore replicas
	replica_mgr_open(&self->replica_mgr);

	// current server is replica
	if (! str_empty(&config()->repl_primary.string))
	{
		self->role = REPL_REPLICA;

		// set to read-only, even if replication is not enabled
		var_int_set(&config()->read_only, true);
	}
}

void
repl_start(Repl* self)
{
	if (var_int_of(&config()->repl))
		return;

	log("replication: start as '%s'", repl_role_of(self->role));
	var_int_set(&config()->repl, true);

	// start replicas
	replica_mgr_start(&self->replica_mgr);
}

void
repl_stop(Repl* self)
{
	if (! var_int_of(&config()->repl))
		return;

	log("replication: stop");
	var_int_set(&config()->repl, false);

	// stop replicas
	replica_mgr_stop(&self->replica_mgr);
}

void
repl_promote(Repl* self, Str* primary_id)
{
	if (! var_int_of(&config()->repl))
		log("replication: is disabled");

	// become replica
	if (primary_id)
	{
		self->role = REPL_REPLICA;

		// set to read-only, even if replication is not enabled
		var_int_set(&config()->read_only, true);

		// set new primary id
		var_string_set(&config()->repl_primary, primary_id);

		log("replication: switch to replica, new primary is '%.*s'", str_size(primary_id),
		    str_of(primary_id));
		return;
	}

	// become primary

	// set to read-write
	var_int_set(&config()->read_only, false);

	// remove primary id
	Str empty;
	str_init(&empty);
	var_string_set(&config()->repl_primary, &empty);

	log("replication: switch to primary");
}
