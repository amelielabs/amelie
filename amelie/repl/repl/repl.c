
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
repl_init(Repl* self, Db* db, RecoverIf* iface, void* iface_arg)
{
	self->role = REPL_PRIMARY;
	replicas_init(&self->replicas, db);
	receiver_init(&self->receiver, db, iface, iface_arg);
}

void
repl_free(Repl* self)
{
	replicas_free(&self->replicas);
	receiver_free(&self->receiver);
}

void
repl_start(Repl* self)
{
	if (opt_int_of(&state()->repl))
		return;

	info("replication: start as '{s}'", repl_role_of(self->role));
	opt_int_set(&state()->repl, true);

	// start replicas
	replicas_start(&self->replicas);

	// start receiver
	receiver_start(&self->receiver);
}

void
repl_stop(Repl* self)
{
	if (! opt_int_of(&state()->repl))
		return;

	info("replication: stop");
	opt_int_set(&state()->repl, false);

	// stop replicas
	replicas_stop(&self->replicas);

	// stop receiver
	receiver_stop(&self->receiver);
}

void
repl_follow(Repl* self, Str* primary_id)
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
		if (uuid_empty(&id))
			error("replication: invalid primary uuid");

		// validate id
		if (uuid_is(&id, opt_uuid_of(&config()->uuid)))
			error("replication: primary id cannot match this server id");

		opt_int_set(&state()->recover, RECOVER_REPL);
		self->role = REPL_REPLICA;

		// set new primary id
		opt_uuid_set(&state()->repl_primary, &id);

		info("replication: switch to replica, new primary is '{str}'",
		     primary_id);
		return;
	}

	// switch to primary

	// remove primary id
	Uuid empty;
	uuid_init(&empty);
	opt_uuid_set(&state()->repl_primary, &empty);

	opt_int_set(&state()->recover, RECOVER_OFF);
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
	if (opt_uuid_empty(&state()->repl_primary))
		encode_null(buf);
	else
		encode_uuid(buf, opt_uuid_of((&state()->repl_primary)));

	encode_raw(buf, "replicas", 8);
	replicas_list(&self->replicas, buf, NULL, 0);

	encode_obj_end(buf);
}

void
repl_describe(Repl* self, Buf* buf)
{
	// start replication
	if (opt_int_of(&state()->repl))
		buf_format(buf, "start replication;\n");

	// follow "uuid"
	if (! opt_uuid_empty(&state()->repl_primary))
	{
		char id[UUID_SZ];
		uuid_get(&state()->repl_primary.uuid, id, sizeof(id));
		buf_format(buf, "follow {qs};\n", id);
	}

	// create replica
	list_foreach(&self->replicas.list)
	{
		auto replica = list_at(Replica, link);

		// id
		char id[UUID_SZ];
		uuid_get(&replica->config->id, id, sizeof(id));

		// uri
		auto uri = buf_create();
		defer_buf(uri);
		uri_export(&replica->config->endpoint, uri);
		buf_format(buf, "create replica {qs} {qbuf};\n", id, uri);
	}
}
