
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
		wal_del(&self->db->wal, &replica->wal_slot);
		replica_free(replica);
	}
}

static inline void
replica_mgr_save(ReplicaMgr* self)
{
	// create dump
	auto buf = buf_begin();
	encode_array(buf);
	list_foreach(&self->list)
	{
		auto replica = list_at(Replica, link);
		replica_config_write(replica->config, buf);
	}
	encode_array_end(buf);

	// update and save state
	var_data_set_buf(&config()->replicas, buf);

	buf_end(buf);
	buf_free(buf);
}

void
replica_mgr_open(ReplicaMgr* self)
{
	auto replicas = &config()->replicas;
	if (! var_data_is_set(replicas))
		return;
	auto pos = var_data_of(replicas);
	if (data_is_null(pos))
		return;

	data_read_array(&pos);
	while (! data_read_array_end(&pos))
	{
		auto config = replica_config_read(&pos);
		guard(replica_config_free, config);

		auto replica = replica_allocate(config, &self->db->wal);
		list_append(&self->list, &replica->link);
		self->list_count++;

		// register wal slot
		wal_add(&self->db->wal, &replica->wal_slot);
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
			uuid_to_string(&config->id, uuid, sizeof(uuid));
			error("replica '%s': already exists", uuid);
		}
		return;
	}
	replica = replica_allocate(config, &self->db->wal);
	list_append(&self->list, &replica->link);
	self->list_count++;
	replica_mgr_save(self);

	// register wal slot
	wal_add(&self->db->wal, &replica->wal_slot);

	// start streamer
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
			uuid_to_string(id, uuid, sizeof(uuid));
			error("replica '%s': not exists", uuid);
		}
		return;
	}
	list_unlink(&replica->link);
	self->list_count--;
	replica_mgr_save(self);

	replica_stop(replica);

	// unregister wal slot
	wal_del(&self->db->wal, &replica->wal_slot);
	replica_free(replica);
}

Buf*
replica_mgr_list(ReplicaMgr* self)
{
	auto buf = buf_begin();
	encode_array(buf);
	list_foreach(&self->list)
	{
		auto replica = list_at(Replica, link);
		replica_config_write(replica->config, buf);
	}
	encode_array_end(buf);
	return buf_end(buf);
}

Replica*
replica_mgr_find(ReplicaMgr* self, Uuid* id)
{
	list_foreach(&self->list)
	{
		auto replica = list_at(Replica, link);
		if (uuid_compare(&replica->config->id, id))
			return replica;
	}
	return NULL;
}
