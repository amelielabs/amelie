
//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// Copyright (c) 2024 Amelie Labs.
//
// AGPL-3.0 Licensed.
//

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_json.h>
#include <amelie_config.h>
#include <amelie_user.h>
#include <amelie_auth.h>
#include <amelie_http.h>
#include <amelie_client.h>
#include <amelie_server.h>
#include <amelie_row.h>
#include <amelie_heap.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_wal.h>
#include <amelie_db.h>
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
	auto primary_id = &state()->repl_primary.string;
	if (! str_empty(primary_id))
	{
		// validate id
		repl_validate_primary(primary_id);

		self->role = REPL_REPLICA;

		// set to read-only, even if replication is not enabled
		opt_int_set(&state()->read_only, true);
	}
}

void
repl_start(Repl* self)
{
	if (opt_int_of(&state()->repl))
		return;

	info("replication: start as '%s'", repl_role_of(self->role));
	opt_int_set(&state()->repl, true);

	// start replicas
	replica_mgr_start(&self->replica_mgr);
}

void
repl_stop(Repl* self)
{
	if (! opt_int_of(&state()->repl))
		return;

	info("replication: stop");
	opt_int_set(&state()->repl, false);

	// stop replicas
	replica_mgr_stop(&self->replica_mgr);
}

void
repl_subscribe(Repl* self, Str* primary_id)
{
	if (! opt_int_of(&state()->repl))
		error("replication: is disabled");

	// switch to replica
	if (primary_id)
	{
		// validate id
		repl_validate_primary(primary_id);
		self->role = REPL_REPLICA;

		// set to read-only, even if replication is not enabled
		opt_int_set(&state()->read_only, true);

		// set new primary id
		opt_string_set(&state()->repl_primary, primary_id);

		info("replication: switch to replica, new primary is '%.*s'", str_size(primary_id),
		     str_of(primary_id));
		return;
	}

	// switch to primary

	// set to read-write
	opt_int_set(&state()->read_only, false);

	// remove primary id
	Str empty;
	str_init(&empty);
	opt_string_set(&state()->repl_primary, &empty);

	self->role = REPL_PRIMARY;
	info("replication: switch to primary");
}

Buf*
repl_status(Repl* self)
{
	// obj
	auto buf = buf_create();
	encode_obj(buf);

	// active
	encode_raw(buf, "active", 6);
	encode_bool(buf, opt_int_of(&state()->repl));

	// role
	encode_raw(buf, "role", 4);
	encode_cstr(buf, repl_role_of(self->role));

	// primary
	encode_raw(buf, "primary", 7);
	if (str_empty(&state()->repl_primary.string))
		encode_null(buf);
	else
		encode_string(buf, &state()->repl_primary.string);

	encode_raw(buf, "replicas", 8);
	auto replicas = replica_mgr_list(&self->replica_mgr, NULL);
	defer_buf(replicas);
	buf_write(buf, replicas->start, buf_size(replicas));

	encode_obj_end(buf);
	return buf;
}
