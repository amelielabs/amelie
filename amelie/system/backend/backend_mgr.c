
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
#include <amelie_value.h>
#include <amelie_set.h>
#include <amelie_content.h>
#include <amelie_executor.h>
#include <amelie_func.h>
#include <amelie_vm.h>
#include <amelie_parser.h>
#include <amelie_planner.h>
#include <amelie_compiler.h>
#include <amelie_frontend.h>
#include <amelie_backend.h>

void
backend_mgr_init(BackendMgr* self, Db* db, FunctionMgr* function_mgr)
{
	self->list_count   = 0;
	self->function_mgr = function_mgr;
	self->db           = db;
	list_init(&self->list);
	router_init(&self->router);
}

void
backend_mgr_free(BackendMgr* self)
{
	// freed by worker_mgr
	unused(self);
	assert(! self->list_count);
}

void
backend_mgr_sync(BackendMgr* self)
{
	list_foreach(&self->list)
	{
		auto backend = list_at(Backend, link);
		backend_sync(backend);
	}
}

static inline Backend*
backend_mgr_find(BackendMgr* self, Uuid* id)
{
	list_foreach(&self->list)
	{
		auto backend = list_at(Backend, link);
		if (! uuid_compare(&backend->worker->id, id))
			return backend;
	}
	return NULL;
}

void
backend_mgr_map(BackendMgr* self, PartMap* map, Part* part)
{
	// find partition by uuid
	auto backend = backend_mgr_find(self, &part->config->backend);
	if (! backend)
		error("partition worker cannot be found");

	// prepare partition mapping
	if (! part_map_created(map))
		part_map_create(map);

	// add route to the mapping if not exists
	auto route = &backend->worker->route;
	part_map_add(map, route);

	// map partition range to the worker order
	part->route = route;
	int i = part->config->min;
	for (; i < part->config->max; i++)
		part_map_set(map, i, route);
}

static void
backend_mgr_if_create(Worker* worker)
{
	BackendMgr* self = worker->iface_arg;

	// create and register backend worker
	auto backend = backend_allocate(worker, self->db, self->function_mgr);
	list_append(&self->list, &backend->link);
	self->list_count++;
	var_int_set(&config()->backends, self->list_count);
	worker->context = backend;

	route_init(&worker->route, &backend->task.channel);
	router_add(&self->router, &worker->route);

	backend_start(backend);
}

static void
backend_mgr_if_free(Worker* worker)
{
	BackendMgr* self = worker->iface_arg;
	Backend* backend = worker->context;
	if (! backend)
		return;

	backend_stop(backend);

	list_unlink(&backend->link);
	self->list_count--;
	var_int_set(&config()->backends, self->list_count);
	router_del(&self->router, &worker->route);

	backend_free(backend);
	worker->context = NULL;
}

WorkerIf backend_mgr_if =
{
	.create = backend_mgr_if_create,
	.free   = backend_mgr_if_free
};
