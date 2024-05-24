
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
#include <sonata_auth.h>
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
#include <sonata_shard.h>

void
shard_mgr_init(ShardMgr* self, Db* db, FunctionMgr* function_mgr)
{
	self->shards       = NULL;
	self->shards_count = 0;
	self->function_mgr = function_mgr;
	self->db           = db;
}

void
shard_mgr_free(ShardMgr* self)
{
	if (self->shards)
	{
		for (int i = 0; i < self->shards_count; i++)
		{
			auto shard = self->shards[i];
			if (shard) {
				shard_stop(shard);
				shard_free(shard);
			}
		}
		so_free(self->shards);
	}
}

static inline Buf*
shard_mgr_dump(ShardMgr* self)
{
	auto buf = buf_begin();
	encode_array(buf);
	for (int i = 0; i < self->shards_count; i++)
	{
		auto shard = self->shards[i];
		shard_config_write(shard->config, buf);
	}
	encode_array_end(buf);
	return buf_end(buf);
}

static inline void
shard_mgr_save(ShardMgr* self)
{
	// create dump
	auto buf = shard_mgr_dump(self);
	guard_buf(buf);

	// update and save state
	var_data_set_buf(&config()->state_shards, buf);
	control_save_config();
}

void
shard_mgr_open(ShardMgr* self)
{
	auto shards = &config()->state_shards;
	if (! var_data_is_set(shards))
		return;
	auto pos = var_data_of(shards);
	if (data_is_null(pos))
		return;
	int count = array_size(pos);

	int allocated = count * sizeof(Shard*);
	self->shards_count = count;
	self->shards = so_malloc(allocated);
	memset(self->shards, 0, allocated);

	int i = 0;
	data_read_array(&pos);
	while (! data_read_array_end(&pos))
	{
		// value
		auto config = shard_config_read(&pos);
		guard(shard_config_free, config);

		auto shard = shard_allocate(config, self->db, self->function_mgr);
		self->shards[i] = shard;

		// set shard order
		shard->order = i;
		i++;
	}
}

void
shard_mgr_create(ShardMgr* self, int count)
{
	int allocated = count * sizeof(Shard*);
	self->shards_count = count;
	self->shards = so_malloc(allocated);
	memset(self->shards, 0, allocated);

	for (int i = 0; i < count; i++)
	{
		// create config
		auto config = shard_config_allocate();
		guard(shard_config_free, config);

		// set shard id
		Uuid id;
		uuid_generate(&id, global()->random);
		shard_config_set_id(config, &id);

		// create shard
		auto shard = shard_allocate(config, self->db, self->function_mgr);
		self->shards[i] = shard;

		// set shard order
		shard->order = i;
	}

	shard_mgr_save(self);
}

void
shard_mgr_set_router(ShardMgr* self, Router* router)
{
	router_create(router, self->shards_count);
	for (int i = 0; i < self->shards_count; i++)
	{
		auto shard = self->shards[i];
		auto route = router_at(router, i);
		route->order   =  shard->order;
		route->channel = &shard->task.channel;
	}
}

void
shard_mgr_set_partition_map(ShardMgr* self, PartMgr* part_mgr)
{
	// create partition map and set each partition range to the shard order
	auto map = &part_mgr->map;
	part_map_create(map);
	list_foreach(&part_mgr->list)
	{
		auto part  = list_at(Part, link);
		auto shard = shard_mgr_find(self, &part->config->shard);
		if (! shard)
			error("partition shard cannot be found");
		int i = part->config->shard_min;
		for (; i < part->config->shard_max; i++)
			part_map_set(map, i, shard->order);
	}
}

Shard*
shard_mgr_find(ShardMgr* self, Uuid* uuid)
{
	for (int i = 0; i < self->shards_count; i++)
	{
		auto shard = self->shards[i];
		if (uuid_compare(&shard->config->id, uuid))
			return shard;
	}
	return NULL;
}

void
shard_mgr_start(ShardMgr* self)
{
	for (int i = 0; i < self->shards_count; i++)
	{
		auto shard = self->shards[i];
		shard_start(shard);
	}
}

void
shard_mgr_stop(ShardMgr* self)
{
	for (int i = 0; i < self->shards_count; i++)
	{
		auto shard = self->shards[i];
		shard_stop(shard);
	}
}

void
shard_mgr_recover(ShardMgr* self)
{
	for (int i = 0; i < self->shards_count; i++)
	{
		auto shard = self->shards[i];
		auto buf = msg_begin(RPC_RECOVER);
		msg_end(buf);
		channel_write(&shard->task.channel, buf);
	}
}
