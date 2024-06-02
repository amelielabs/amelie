
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
#include <sonata_cluster.h>

void
cluster_init(Cluster* self, Db* db, FunctionMgr* function_mgr)
{
	self->list_count   = 0;
	self->function_mgr = function_mgr;
	self->db           = db;
	list_init(&self->list);
	router_init(&self->router);
}

void
cluster_free(Cluster* self)
{
	list_foreach_safe(&self->list)
	{
		auto node = list_at(Node, link);
		node_stop(node);
		node_free(node);
	}
	router_free(&self->router);
}

static inline void
cluster_save(Cluster* self)
{
	// create dump
	auto buf = buf_begin();
	encode_array(buf);
	list_foreach(&self->list)
	{
		auto node = list_at(Node, link);
		node_config_write(node->config, buf);
	}
	encode_array_end(buf);

	// update and save state
	var_data_set_buf(&config()->nodes, buf);

	buf_end(buf);
	buf_free(buf);
}

static void
cluster_add(Cluster* self, NodeConfig* config)
{
	auto node = node_allocate(config, self->db, self->function_mgr);
	node->order = self->list_count;
	list_append(&self->list, &node->link);
	self->list_count++;
}

static void
cluster_bootstrap(Cluster* self)
{
	// precreate compute nodes
	auto shards = var_int_of(&config()->shards);
	while (shards-- > 0)
	{
		auto config = node_config_allocate();
		guard(node_config_free, config);

		// set type
		node_config_set_type(config, NODE_COMPUTE);

		// set node id
		Uuid id;
		uuid_generate(&id, global()->random);
		node_config_set_id(config, &id);

		cluster_add(self, config);
	}

	cluster_save(self);
	control_save_config();
}

void
cluster_open(Cluster* self, bool bootstrap)
{
	// precreate or restore nodes
	if (! bootstrap)
	{
		auto nodes = &config()->nodes;
		if (! var_data_is_set(nodes))
			return;
		auto pos = var_data_of(nodes);
		if (data_is_null(pos))
			return;
		data_read_array(&pos);
		while (! data_read_array_end(&pos))
		{
			auto config = node_config_read(&pos);
			guard(node_config_free, config);
			cluster_add(self, config);
		}
	} else
	{
		cluster_bootstrap(self);
	}

	// prepare router
	router_create(&self->router, self->list_count);

	list_foreach(&self->list)
	{
		auto node = list_at(Node, link);
		auto route = router_at(&self->router, node->order);
		route->channel = &node->task.channel;
	}
}

void
cluster_set_partition_map(Cluster* self, PartMgr* part_mgr)
{
	// create partition map and set each partition range to the node order
	auto map = &part_mgr->map;
	part_map_create(map);
	list_foreach(&part_mgr->list)
	{
		auto part  = list_at(Part, link);
		auto node = cluster_find(self, &part->config->node);
		if (! node)
			error("partition node cannot be found");
		int i = part->config->min;
		for (; i < part->config->max; i++)
			part_map_set(map, i, node->order);
	}
}

void
cluster_start(Cluster* self)
{
	list_foreach(&self->list)
	{
		auto node = list_at(Node, link);
		node_start(node);
	}
}

void
cluster_stop(Cluster* self)
{
	list_foreach(&self->list)
	{
		auto node = list_at(Node, link);
		node_stop(node);
	}
}

void
cluster_recover(Cluster* self)
{
	list_foreach(&self->list)
	{
		auto node = list_at(Node, link);
		auto buf = msg_begin(RPC_RECOVER);
		msg_end(buf);
		channel_write(&node->task.channel, buf);
	}
}

Node*
cluster_find(Cluster* self, Uuid* uuid)
{
	list_foreach(&self->list)
	{
		auto node = list_at(Node, link);
		if (uuid_compare(&node->config->id, uuid))
			return node;
	}
	return NULL;
}

void
cluster_create(Cluster* self, NodeConfig* config, bool if_not_exists)
{
	auto node = cluster_find(self, &config->id);
	if (node)
	{
		if (! if_not_exists)
		{
			char uuid[UUID_SZ];
			uuid_to_string(&config->id, uuid, sizeof(uuid));
			error("node '%s': already exists", uuid);
		}
		return;
	}
	cluster_add(self, config);
	cluster_save(self);

	// todo: node order?
	// todo: update router
}

#if 0
void
cluster_drop(Cluster* self, Uuid* id, bool if_exists)
{
	(void)self;
	(void)id;
	(void)if_exists;
	// todo:

	// todo: update router

	cluster_save(self);
}

void
cluster_alter(Cluster* self, NodeConfig* config, bool if_exists)
{
	(void)self;
	(void)config;
	(void)if_exists;

	// todo:

	cluster_save(self);
}
#endif
