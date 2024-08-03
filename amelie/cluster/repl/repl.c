
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
#include <amelie_planner.h>
#include <amelie_compiler.h>
#include <amelie_backup.h>
#include <amelie_repl.h>

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

static void
repl_validate_primary(Str* primary_id)
{
	if (str_compare(primary_id, &config()->uuid.string))
		error("repl: primary id cannot match this server id");
}

void
repl_open(Repl* self)
{
	// restore replicas
	replica_mgr_open(&self->replica_mgr);

	// current server is replica
	auto primary_id = &config()->repl_primary.string;
	if (! str_empty(primary_id))
	{
		// validate id
		repl_validate_primary(primary_id);

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

	info("replication: start as '%s'", repl_role_of(self->role));
	var_int_set(&config()->repl, true);

	// start replicas
	replica_mgr_start(&self->replica_mgr);
}

void
repl_stop(Repl* self)
{
	if (! var_int_of(&config()->repl))
		return;

	info("replication: stop");
	var_int_set(&config()->repl, false);

	// stop replicas
	replica_mgr_stop(&self->replica_mgr);
}

void
repl_promote(Repl* self, Str* primary_id)
{
	if (! var_int_of(&config()->repl))
		error("replication: is disabled");

	// switch to replica
	if (primary_id)
	{
		// validate id
		repl_validate_primary(primary_id);
		self->role = REPL_REPLICA;

		// set to read-only, even if replication is not enabled
		var_int_set(&config()->read_only, true);

		// set new primary id
		var_string_set(&config()->repl_primary, primary_id);

		info("replication: switch to replica, new primary is '%.*s'", str_size(primary_id),
		     str_of(primary_id));
		return;
	}

	// switch to primary

	// set to read-write
	var_int_set(&config()->read_only, false);

	// remove primary id
	Str empty;
	str_init(&empty);
	var_string_set(&config()->repl_primary, &empty);

	self->role = REPL_PRIMARY;
	info("replication: switch to primary");
}

Buf*
repl_show(Repl* self)
{
	// map
	auto buf = buf_begin();
	encode_map(buf);

	// active
	encode_raw(buf, "active", 6);
	encode_bool(buf, var_int_of(&config()->repl));

	// role
	encode_raw(buf, "role", 4);
	encode_cstr(buf, repl_role_of(self->role));

	// primary
	encode_raw(buf, "primary", 7);
	if (str_empty(&config()->repl_primary.string))
		encode_null(buf);
	else
		encode_string(buf, &config()->repl_primary.string);

	encode_map_end(buf);
	return buf_end(buf);
}
