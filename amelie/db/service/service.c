
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
#include <amelie_row.h>
#include <amelie_transaction.h>
#include <amelie_storage.h>
#include <amelie_object.h>
#include <amelie_tier.h>
#include <amelie_heap.h>
#include <amelie_index.h>
#include <amelie_part.h>
#include <amelie_catalog.h>
#include <amelie_wal.h>
#include <amelie_service.h>

typedef struct ServiceWorker ServiceWorker;

struct ServiceWorker
{
	Service* service;
	int64_t  coroutine_id;
	List     link;
};

static inline ServiceWorker*
service_worker_allocate(Service* service)
{
	auto self = (ServiceWorker*)am_malloc(sizeof(ServiceWorker));
	self->service      = service;
	self->coroutine_id = -1;
	list_init(&self->link);
	return self;
}

static inline void
service_worker_free(ServiceWorker* self)
{
	am_free(self);
}

static void
service_schedule(Table* table, Action* action)
{
	// take shared table lock
	auto lock_table = lock(&table->rel, LOCK_SHARED);
	defer(unlock, lock_table);

	// schedule eviction
	auto config = &table->config->partitioning;
	unused(config);
	unused(action);
}

void
service_execute(Service* self, ServiceWorker* worker, Action* action)
{
	// take shared catalog lock
	auto lock_catalog = lock_system(REL_CATALOG, LOCK_SHARED);
	defer(unlock, lock_catalog);

	// find table
	auto table = table_mgr_find_by(&self->catalog->table_mgr, &action->req->id_table, false);
	if (! table)
		return;

	// find table and schedule next service action
	service_schedule(table, action);

	// execute
	switch (action->type) {
	case ACTION_NONE:
		unused(worker);
		break;
	}
}

static void
service_main(void* arg)
{
	ServiceWorker* self = arg;
	auto queue = &self->service->queue;

	Action action;
	action_init(&action);
	for (;;)
	{
		auto shutdown = service_queue_next(queue, &action);
		if (unlikely(shutdown))
			break;
		error_catch (
			service_execute(self->service, self, &action)
		);
		service_queue_complete(queue, &action);
	}
}

void
service_init(Service* self, Catalog* catalog, WalMgr* wal_mgr)
{
	self->workers_count = 0;
	self->catalog       = catalog;
	self->wal_mgr       = wal_mgr;
	service_lock_mgr_init(&self->lock_mgr);
	service_queue_init(&self->queue);
	list_init(&self->workers);
}

void
service_free(Service* self)
{
	service_queue_free(&self->queue);
	service_lock_mgr_free(&self->lock_mgr);
}

void
service_start(Service* self)
{
	auto count = opt_int_of(&config()->jobs);
	while (count-- > 0)
	{
		auto worker = service_worker_allocate(self);
		worker->coroutine_id = coroutine_create(service_main, worker);
		list_append(&self->workers, &worker->link);
		self->workers_count++;
	}
}

void
service_stop(Service* self)
{
	// shutdown workers
	service_queue_shutdown(&self->queue);

	// wait for completion
	list_foreach_safe(&self->workers)
	{
		auto worker = list_at(ServiceWorker, link);
		coroutine_wait(worker->coroutine_id);
		service_worker_free(worker);
	}
	self->workers_count = 0;
	list_init(&self->workers);
}
