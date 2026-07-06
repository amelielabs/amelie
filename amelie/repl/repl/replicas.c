
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

void
replicas_init(Replicas* self, Db* db)
{
	self->db         = db;
	self->list_count = 0;
	list_init(&self->list);
}

void
replicas_free(Replicas* self)
{
	list_foreach_safe(&self->list)
	{
		auto replica = list_at(Replica, link);
		wal_detach(&self->db->wal, &replica->wal_slot);
		replica_free(replica);
	}
}

static inline void
replicas_save(Replicas* self)
{
	// create dump
	auto buf = buf_create();
	defer_buf(buf);

	encode_array(buf);
	list_foreach(&self->list)
	{
		auto replica = list_at(Replica, link);
		replica_config_write(replica->config, buf, 0);
	}
	encode_array_end(buf);

	// update and save state
	opt_json_set_buf(&state()->replicas, buf);
}

void
replicas_open(Replicas* self)
{
	auto replicas = &state()->replicas;
	if (opt_json_empty(replicas))
		return;
	auto pos = opt_json_of(replicas);
	if (data_is_null(pos))
		return;

	unpack_array(&pos);
	while (! unpack_array_end(&pos))
	{
		auto config = replica_config_read(&pos);
		defer(replica_config_free, config);

		auto replica = replica_allocate(config, &self->db->wal);
		list_append(&self->list, &replica->link);
		self->list_count++;

		// register wal slot
		wal_attach(&self->db->wal, &replica->wal_slot);
	}
}

void
replicas_start(Replicas* self)
{
	list_foreach(&self->list)
	{
		auto replica = list_at(Replica, link);
		replica_start(replica);
	}
}

void
replicas_stop(Replicas* self)
{
	list_foreach(&self->list)
	{
		auto replica = list_at(Replica, link);
		replica_stop(replica);
	}
}

void
replicas_create(Replicas* self, ReplicaConfig* config, bool if_not_exists)
{
	auto replica = replicas_find(self, &config->id);
	if (replica)
	{
		if (! if_not_exists)
		{
			char uuid[UUID_SZ];
			uuid_get(&config->id, uuid, sizeof(uuid));
			error("replica '{s}': already exists", uuid);
		}
		return;
	}
	replica = replica_allocate(config, &self->db->wal);
	list_append(&self->list, &replica->link);
	self->list_count++;
	replicas_save(self);

	// register wal slot
	wal_attach(&self->db->wal, &replica->wal_slot);

	// start streamer
	if (opt_int_of(&state()->repl))
		replica_start(replica);
}

void
replicas_drop(Replicas* self, Uuid* id, bool if_exists)
{
	auto replica = replicas_find(self, id);
	if (! replica)
	{
		if (! if_exists)
		{
			char uuid[UUID_SZ];
			uuid_get(id, uuid, sizeof(uuid));
			error("replica '{s}': not exists", uuid);
		}
		return;
	}
	list_unlink(&replica->link);
	self->list_count--;
	replicas_save(self);

	// stop streamer
	if (opt_int_of(&state()->repl))
		replica_stop(replica);

	// unregister wal slot
	wal_detach(&self->db->wal, &replica->wal_slot);
	replica_free(replica);
}

void
replicas_list(Replicas* self, Buf* buf, Uuid* id, int flags)
{
	if (id)
	{
		auto replica = replicas_find(self, id);
		if (! replica)
			encode_null(buf);
		else
			replica_status(replica, buf, flags);
	} else
	{
		encode_array(buf);
		list_foreach(&self->list)
		{
			auto replica = list_at(Replica, link);
			replica_status(replica, buf, flags);
		}
		encode_array_end(buf);
	}
}

Replica*
replicas_find(Replicas* self, Uuid* id)
{
	list_foreach(&self->list)
	{
		auto replica = list_at(Replica, link);
		if (! uuid_compare(&replica->config->id, id))
			return replica;
	}
	return NULL;
}
