
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

#include <amelie_runtime>
#include <amelie_server>
#include <amelie_db>
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
repl_validate_primary(Uuid* primary_id)
{
	if (uuid_is(primary_id, opt_uuid_of(&config()->uuid)))
		error("repl: primary id cannot match this server id");
}

void
repl_open(Repl* self)
{
	// restore replicas
	replica_mgr_open(&self->replica_mgr);

	// current server is replica
	auto primary_id = opt_uuid_of(&state()->repl_primary_);
	if (! uuid_empty(primary_id))
	{
		// validate id
		repl_validate_primary(primary_id);

		self->role = REPL_REPLICA;
	}
}

void
repl_start(Repl* self)
{
	if (opt_int_of(&state()->repl))
		return;

	info("replication: start as '{s}'", repl_role_of(self->role));
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
		Uuid id;
		uuid_init(&id);
		if (uuid_set_nothrow(&id, primary_id) == -1)
			error("replication: invalid primary uuid");

		// validate id
		repl_validate_primary(&id);
		self->role = REPL_REPLICA;

		// set new primary id
		opt_uuid_set(&state()->repl_primary_, &id);

		info("replication: switch to replica, new primary is '{str}'",
		     primary_id);
		return;
	}

	// switch to primary

	// remove primary id
	Uuid empty;
	uuid_init(&empty);
	opt_uuid_set(&state()->repl_primary_, &empty);

	self->role = REPL_PRIMARY;
	info("replication: switch to primary");
}

void
repl_status(Repl* self, Buf* buf)
{
	// obj
	encode_obj(buf);

	// active
	encode_raw(buf, "active", 6);
	encode_bool(buf, opt_int_of(&state()->repl));

	// role
	encode_raw(buf, "role", 4);
	encode_cstr(buf, repl_role_of(self->role));

	// primary
	encode_raw(buf, "primary", 7);
	if (opt_uuid_empty(&state()->repl_primary_))
		encode_null(buf);
	else
		encode_uuid(buf, opt_uuid_of((&state()->repl_primary_)));

	encode_raw(buf, "replicas", 8);
	replica_mgr_list(&self->replica_mgr, buf, NULL, 0);

	encode_obj_end(buf);
}
