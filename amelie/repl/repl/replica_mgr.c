
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

void
replica_mgr_init(ReplicaMgr* self, Db* db)
{
	self->db = db;
	self->list_count = 0;
	list_init(&self->list);
}

void
replica_mgr_free(ReplicaMgr* self)
{
	list_foreach_safe(&self->list)
	{
		auto replica = list_at(Replica, link);
		wal_del(&self->db->wal_mgr.wal, &replica->wal_slot);
		replica_free(replica);
	}
}

static inline void
replica_mgr_save(ReplicaMgr* self)
{
	// create dump
	auto buf = buf_create();
	defer_buf(buf);

	encode_array(buf);
	list_foreach(&self->list)
	{
		auto replica = list_at(Replica, link);
		replica_config_write(replica->config, buf);
	}
	encode_array_end(buf);

	// update and save state
	opt_json_set_buf(&state()->replicas, buf);
}

void
replica_mgr_open(ReplicaMgr* self)
{
	auto replicas = &state()->replicas;
	if (! opt_json_is_set(replicas))
		return;
	auto pos = opt_json_of(replicas);
	if (json_is_null(pos))
		return;

	json_read_array(&pos);
	while (! json_read_array_end(&pos))
	{
		auto config = replica_config_read(&pos);
		defer(replica_config_free, config);

		auto replica = replica_allocate(config, &self->db->wal_mgr.wal);
		list_append(&self->list, &replica->link);
		self->list_count++;

		// register wal slot
		wal_add(&self->db->wal_mgr.wal, &replica->wal_slot);
	}
}

void
replica_mgr_start(ReplicaMgr* self)
{
	list_foreach(&self->list)
	{
		auto replica = list_at(Replica, link);
		replica_start(replica);
	}
}

void
replica_mgr_stop(ReplicaMgr* self)
{
	list_foreach(&self->list)
	{
		auto replica = list_at(Replica, link);
		replica_stop(replica);
	}
}

void
replica_mgr_create(ReplicaMgr* self, ReplicaConfig* config, bool if_not_exists)
{
	auto replica = replica_mgr_find(self, &config->id);
	if (replica)
	{
		if (! if_not_exists)
		{
			char uuid[UUID_SZ];
			uuid_get(&config->id, uuid, sizeof(uuid));
			error("replica '%s': already exists", uuid);
		}
		return;
	}
	replica = replica_allocate(config, &self->db->wal_mgr.wal);
	list_append(&self->list, &replica->link);
	self->list_count++;
	replica_mgr_save(self);

	// register wal slot
	wal_add(&self->db->wal_mgr.wal, &replica->wal_slot);

	// start streamer
	if (opt_int_of(&state()->repl))
		replica_start(replica);
}

void
replica_mgr_drop(ReplicaMgr* self, Uuid* id, bool if_exists)
{
	auto replica = replica_mgr_find(self, id);
	if (! replica)
	{
		if (! if_exists)
		{
			char uuid[UUID_SZ];
			uuid_get(id, uuid, sizeof(uuid));
			error("replica '%s': not exists", uuid);
		}
		return;
	}
	list_unlink(&replica->link);
	self->list_count--;
	replica_mgr_save(self);

	// stop streamer
	if (opt_int_of(&state()->repl))
		replica_stop(replica);

	// unregister wal slot
	wal_del(&self->db->wal_mgr.wal, &replica->wal_slot);
	replica_free(replica);
}

Buf*
replica_mgr_list(ReplicaMgr* self, Uuid* id)
{
	auto buf = buf_create();
	if (id)
	{
		auto replica = replica_mgr_find(self, id);
		if (! replica)
			encode_null(buf);
		else
			replica_status(replica, buf);
	} else
	{
		encode_array(buf);
		list_foreach(&self->list)
		{
			auto replica = list_at(Replica, link);
			replica_status(replica, buf);
		}
		encode_array_end(buf);
	}
	return buf;
}

Replica*
replica_mgr_find(ReplicaMgr* self, Uuid* id)
{
	list_foreach(&self->list)
	{
		auto replica = list_at(Replica, link);
		if (! uuid_compare(&replica->config->id, id))
			return replica;
	}
	return NULL;
}
