
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
service_next(Table* table, Action* action)
{
	// take shared table lock
	auto lock_table = lock(&table->rel, LOCK_SHARED);
	defer(unlock, lock_table);

	// todo:
	auto config = &table->config->partitioning;
	unused(config);
	unused(action);
}

void
service_relation(ServiceWorker* self, Action* action)
{
	auto service = self->service;

	// take shared catalog lock
	auto lock_catalog = lock_system(REL_CATALOG, LOCK_SHARED);
	defer(unlock, lock_catalog);

	// find table
	auto table = table_mgr_find_by(&service->catalog->table_mgr, &action->req->id_table, false);
	if (! table)
		return;

	//  find table and schedule next service action
	service_next(table, action);

	// execute
	switch (action->type) {
	case ACTION_NONE:
		break;
	}
}

static void
service_wal_sync_job(intptr_t* argv)
{
	auto service = (Service*)argv[0];
	auto wal     = &service->wal_mgr->wal;
	wal_sync(wal, false);
}

static void
service_wal_create_job(intptr_t* argv)
{
	auto service = (Service*)argv[0];
	auto wal     = &service->wal_mgr->wal;
	if (opt_int_of(&config()->wal_sync_on_close))
		wal_sync(wal, false);
	wal_create(wal, state_lsn() + 1);
	if (opt_int_of(&config()->wal_sync_on_create))
		wal_sync(wal, true);
}

static void
service_execute(ServiceWorker* self, int op, Action* action)
{
	Service* service = self->service;
	switch (op) {
	case SERVICE_WAL_SYNC:
		run(service_wal_sync_job, 1, service);
		break;
	case SERVICE_WAL_CREATE:
		run(service_wal_create_job, 1, service);
		break;
	case SERVICE_CHECKPOINT:
		service_checkpoint(service);
		break;
	case SERVICE_RELATION:
		service_relation(self, action);
		break;
	}
}

static void
service_main(void* arg)
{
	ServiceWorker* self  = arg;
	auto           queue = &self->service->queue;

	Action action;
	action_init(&action);
	for (;;)
	{
		auto op = service_queue_next(queue, &action);
		if (op == -1)
			break;
		service_execute(self, op, &action);
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
