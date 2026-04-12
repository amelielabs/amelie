
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
#include <amelie_cdc.h>
#include <amelie_storage.h>
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
service_main(void* arg)
{
	ServiceWorker* self = arg;
	while (service_step(self->service))
	{ }
}

void
service_init(Service* self, Catalog* catalog, Wal* wal)
{
	self->workers_count = 0;
	self->catalog       = catalog;
	self->wal           = wal;
	service_lock_mgr_init(&self->lock_mgr);
	action_mgr_init(&self->action_mgr);
	list_init(&self->workers);
}

void
service_free(Service* self)
{
	action_mgr_free(&self->action_mgr);
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
	action_mgr_shutdown(&self->action_mgr);

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
