
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
	Evict    evict;
	Merge    merge;
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
	evict_init(&self->evict, service);
	merge_init(&self->merge, service);
	list_init(&self->link);
	return self;
}

static inline void
service_worker_free(ServiceWorker* self)
{
	evict_free(&self->evict);
	merge_free(&self->merge);
	am_free(self);
}

static void
service_schedule_evict(Table* table, Action* action)
{
	auto config = &table->config->partitioning;
	auto req = action->req;

	// eviction
	auto cache_usage = atomic_u64_of(&table->part_mgr.heap_total);
	auto start = (uint64_t)(config->cache_size * config->cache_evict_wm) / 100;
	auto stop  = (uint64_t)(config->cache_size * config->cache_evict)    / 100;
	if (req->evict)
	{
		// disable eviction on reaching the target or if the global memory
		// usage drops below threshold
		if (cache_usage <= stop)
			req->evict = false;
	} else
	{
		// start eviction if the total current cache usage reaches cache_evict_wm
		// percents out of the total cache size
		if (cache_usage >= start)
			req->evict = true;
	}
	if (! req->evict)
		return;

	// get the largest partition
	Part* part = NULL;
	list_foreach(&table->part_mgr.list)
	{
		auto ref = list_at(Part, link);
		if (!part || ref->heap->header->size_used > part->heap->header->size_used)
			part = ref;
	}
	assert(part);

	// evict up to cache_evict percent from the partition heap or less,
	// if the total cache usage drops less than that
	auto cache_used_part = part->heap->header->size_used;
	auto size = (uint64_t)(cache_used_part * config->cache_evict) / 100;
	if (stop - cache_usage < size)
		size = stop - cache_usage;

	// stop eviction, if the size is <= cache_evict_min to prevent
	// creating small branches
	if (size <= (uint64_t)config->cache_evict_min)
	{
		req->evict = false;
		return;
	}

	action->type  = ACTION_EVICT;
	action->id    = part->id.id;
	action->evict = size;
}

static void
service_schedule(Table* table, Action* action)
{
	// take shared table lock
	auto lock_table = lock(&table->rel, LOCK_SHARED);
	defer(unlock, lock_table);

	// schedule eviction
	auto config = &table->config->partitioning;
	if (config->cache)
	{
		service_schedule_evict(table, action);
		if (action->type != ACTION_NONE)
			return;
	}

	if (! tier_mgr_created(&table->tier_mgr))
		return;

	// schedule object merge (refresh/split)
	Object* match = NULL;
	auto tier = tier_mgr_first(&table->tier_mgr);
	list_foreach(&tier->list)
	{
		auto object = list_at(Object, link);
		if (!match || object->branches_count > match->branches_count)
			match = object;
	}

	if (match && match->branches_count > tier->config->branches)
	{
		action->type = ACTION_MERGE;
		action->id   = match->id.id;
	}
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
	case ACTION_EVICT:
		if (! evict_run(&worker->evict, table, action->id, action->evict))
			break;
		service_gc(self);
		break;
	case ACTION_MERGE:
		if (! merge_run(&worker->merge, table, action->id))
			break;
		break;
	case ACTION_NONE:
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
