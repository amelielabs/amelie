
//
// amelie.
//
// Real-Time SQL Database.
//

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_data.h>
#include <amelie_config.h>
#include <amelie_user.h>
#include <amelie_http.h>
#include <amelie_client.h>
#include <amelie_server.h>
#include <amelie_row.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_value.h>
#include <amelie_aggr.h>
#include <amelie_executor.h>
#include <amelie_vm.h>
#include <amelie_parser.h>
#include <amelie_planner.h>
#include <amelie_compiler.h>
#include <amelie_backup.h>
#include <amelie_repl.h>
#include <amelie_cluster.h>

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

static Node*
cluster_add(Cluster* self, NodeConfig* config)
{
	auto node = node_allocate(config, self->db, self->function_mgr);
	list_append(&self->list, &node->link);
	self->list_count++;

	router_add(&self->router, &node->route);
	return node;
}

static void
cluster_del(Cluster* self, Node* node)
{
	list_unlink(&node->link);
	self->list_count--;

	router_del(&self->router, &node->route);
	node_free(node);
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
	if (bootstrap)
	{
		cluster_bootstrap(self);
		return;
	}

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
	node = cluster_add(self, config);
	cluster_save(self);

	node_start(node);
}

void
cluster_drop(Cluster* self, Uuid* id, bool if_exists)
{
	char uuid[UUID_SZ];
	uuid_to_string(id, uuid, sizeof(uuid));

	auto node = cluster_find(self, id);
	if (! node)
	{
		if (! if_exists)
			error("node '%s': not exists", uuid);
		return;
	}
	if (node->route.refs > 0)
		error("node '%s': has dependencies", uuid);
	node_stop(node);

	cluster_del(self, node);
	cluster_save(self);
}

void
cluster_map(Cluster* self, PartMap* map, Part* part)
{
	// map partition range to the node order
	auto node = cluster_find(self, &part->config->node);
	if (! node)
		error("partition node cannot be found");
	part->route = &node->route;
	int i = part->config->min;
	for (; i < part->config->max; i++)
		part_map_set(map, i, &node->route);
}

Buf*
cluster_list(Cluster* self)
{
	auto buf = buf_begin();
	encode_array(buf);
	list_foreach(&self->list)
	{
		auto node = list_at(Node, link);
		node_config_write(node->config, buf);
	}
	encode_array_end(buf);
	return buf_end(buf);
}
