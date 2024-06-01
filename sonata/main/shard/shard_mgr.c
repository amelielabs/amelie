
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
#include <sonata_backup.h>
#include <sonata_repl.h>
#include <sonata_shard.h>

void
shard_mgr_init(ShardMgr* self, Db* db, FunctionMgr* function_mgr)
{
	self->shards_count = 0;
	self->function_mgr = function_mgr;
	self->db           = db;
	list_init(&self->shards);
}

void
shard_mgr_free(ShardMgr* self)
{
	list_foreach_safe(&self->shards)
	{
		auto shard = list_at(Shard, link);
		shard_stop(shard);
		shard_free(shard);
	}
}

void
shard_mgr_open(ShardMgr* self, NodeMgr* node_mgr)
{
	int order = 0;
	list_foreach(&node_mgr->list)
	{
		auto node = list_at(Node, link);
		if (node->config->type != NODE_COMPUTE)
			continue;
		auto shard = shard_allocate(node, self->db, self->function_mgr);
		shard->order = order++;
		list_append(&self->shards, &shard->link);
		self->shards_count++;
	}
}

void
shard_mgr_set_router(ShardMgr* self, Router* router)
{
	router_create(router, self->shards_count);
	list_foreach(&self->shards)
	{
		auto shard = list_at(Shard, link);
		auto route = router_at(router, shard->order);
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
		auto shard = shard_mgr_find(self, &part->config->node);
		if (! shard)
			error("partition shard cannot be found");
		int i = part->config->min;
		for (; i < part->config->max; i++)
			part_map_set(map, i, shard->order);
	}
}

Shard*
shard_mgr_find(ShardMgr* self, Uuid* uuid)
{
	list_foreach(&self->shards)
	{
		auto shard = list_at(Shard, link);
		if (uuid_compare(&shard->node->config->id, uuid))
			return shard;
	}
	return NULL;
}

void
shard_mgr_start(ShardMgr* self)
{
	list_foreach(&self->shards)
	{
		auto shard = list_at(Shard, link);
		shard_start(shard);
	}
}

void
shard_mgr_stop(ShardMgr* self)
{
	list_foreach(&self->shards)
	{
		auto shard = list_at(Shard, link);
		shard_stop(shard);
	}
}

void
shard_mgr_recover(ShardMgr* self)
{
	list_foreach(&self->shards)
	{
		auto shard = list_at(Shard, link);
		auto buf = msg_begin(RPC_RECOVER);
		msg_end(buf);
		channel_write(&shard->task.channel, buf);
	}
}
