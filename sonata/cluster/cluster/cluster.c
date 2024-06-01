
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
	node_mgr_init(&self->node_mgr);
}

void
cluster_free(Cluster* self)
{
	list_foreach_safe(&self->list)
	{
		auto backend = list_at(Backend, link);
		backend_stop(backend);
		backend_free(backend);
	}
	router_free(&self->router);
	node_mgr_free(&self->node_mgr);
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

		node_mgr_create(&self->node_mgr, config, false);
	}

	control_save_config();
}

void
cluster_open(Cluster* self, bool bootstrap)
{
	if (bootstrap)
	{
		// precreate compute nodes
		cluster_bootstrap(self);
	} else
	{
		// restore nodes
		node_mgr_open(&self->node_mgr);
	}

	// prepare backends
	list_foreach(&self->node_mgr.list)
	{
		auto node = list_at(Node, link);
		if (node->config->type != NODE_COMPUTE)
			continue;
		auto backend = backend_allocate(node, self->db, self->function_mgr);
		backend->order = self->list_count;
		list_append(&self->list, &backend->link);
		self->list_count++;
	}

	// prepare router
	router_create(&self->router, self->list_count);

	list_foreach(&self->list)
	{
		auto backend = list_at(Backend, link);
		auto route = router_at(&self->router, backend->order);
		route->order =  backend->order;
		route->channel = &backend->task.channel;
	}
}

void
cluster_set_partition_map(Cluster* self, PartMgr* part_mgr)
{
	// create partition map and set each partition range to the backend order
	auto map = &part_mgr->map;
	part_map_create(map);
	list_foreach(&part_mgr->list)
	{
		auto part  = list_at(Part, link);
		auto backend = cluster_find(self, &part->config->node);
		if (! backend)
			error("partition backend cannot be found");
		int i = part->config->min;
		for (; i < part->config->max; i++)
			part_map_set(map, i, backend->order);
	}
}

void
cluster_start(Cluster* self)
{
	list_foreach(&self->list)
	{
		auto backend = list_at(Backend, link);
		backend_start(backend);
	}
}

void
cluster_stop(Cluster* self)
{
	list_foreach(&self->list)
	{
		auto backend = list_at(Backend, link);
		backend_stop(backend);
	}
}

void
cluster_recover(Cluster* self)
{
	list_foreach(&self->list)
	{
		auto backend = list_at(Backend, link);
		auto buf = msg_begin(RPC_RECOVER);
		msg_end(buf);
		channel_write(&backend->task.channel, buf);
	}
}

void
cluster_create(Cluster* self, NodeConfig* config, bool if_not_exists)
{
	auto node = node_mgr_create(&self->node_mgr, config, if_not_exists);
	if (! node)
		return;
	auto backend = backend_allocate(node, self->db, self->function_mgr);
	backend->order = self->list_count;
	list_append(&self->list, &backend->link);
	self->list_count++;

	// todo: update router
}

void
cluster_drop(Cluster* self, Uuid* id, bool if_exists)
{
	(void)self;
	(void)id;
	(void)if_exists;
	// todo:

	// todo: update router
}

void
cluster_alter(Cluster* self, NodeConfig* config, bool if_exists)
{
	(void)self;
	(void)config;
	(void)if_exists;

	// todo:
}

Backend*
cluster_find(Cluster* self, Uuid* uuid)
{
	list_foreach(&self->list)
	{
		auto backend = list_at(Backend, link);
		if (uuid_compare(&backend->node->config->id, uuid))
			return backend;
	}
	return NULL;
}
