
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
#include <amelie_auth.h>
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
	// freed by node_mgr
	assert(! self->list_count);
}

void
cluster_sync(Cluster* self)
{
	list_foreach(&self->list)
	{
		auto compute = list_at(Compute, link);
		compute_sync(compute);
	}
}

static inline Compute*
cluster_find(Cluster* self, Uuid* id)
{
	list_foreach(&self->list)
	{
		auto compute = list_at(Compute, link);
		if (uuid_compare(&compute->node->id, id))
			return compute;
	}
	return NULL;
}

void
cluster_map(Cluster* self, PartMap* map, Part* part)
{
	// find partition by uuid
	auto compute = cluster_find(self, &part->config->node);
	if (! compute)
		error("partition node cannot be found");

	// prepare partition mapping
	if (! part_map_created(map))
		part_map_create(map);

	// add route to the mapping if not exists
	auto route = &compute->node->route;
	part_map_add(map, route);

	// map partition range to the node order
	part->route = route;
	int i = part->config->min;
	for (; i < part->config->max; i++)
		part_map_set(map, i, route);
}

static void
cluster_if_create(Node* node)
{
	Cluster* self = node->iface_arg;

	// create and register compute node
	auto compute = compute_allocate(node, self->db, self->function_mgr);
	list_append(&self->list, &compute->link);
	self->list_count++;
	var_int_set(&config()->backends, self->list_count);
	node->context = compute;

	route_init(&node->route, &compute->task.channel);
	router_add(&self->router, &node->route);

	compute_start(compute);
}

static void
cluster_if_free(Node* node)
{
	Cluster* self = node->iface_arg;
	Compute* compute = node->context;
	if (! compute)
		return;

	compute_stop(compute);

	list_unlink(&compute->link);
	self->list_count--;
	var_int_set(&config()->backends, self->list_count);
	router_del(&self->router, &node->route);

	compute_free(compute);
	node->context = NULL;
}

NodeIf cluster_if =
{
	.create = cluster_if_create,
	.free   = cluster_if_free
};
