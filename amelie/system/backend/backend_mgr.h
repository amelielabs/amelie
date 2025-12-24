#pragma once

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

typedef struct BackendMgr BackendMgr;

struct BackendMgr
{
	Backend** workers;
	int       workers_count;
};

static inline void
backend_mgr_init(BackendMgr* self)
{
	self->workers       = NULL;
	self->workers_count = 0;
}

static inline void
backend_mgr_set_affinity(BackendMgr* self)
{
	auto cpus = get_nprocs();
	auto workers_fe = (int)opt_int_of(&config()->frontends);
	auto workers = workers_fe + self->workers_count;
	if (workers > cpus)
	{
		info("cpu_affinity: the total number of frontend and backends workers is more then "
		     "the number of available cpu cores, skipping.");
		return;
	}
	auto cpu = workers_fe;
	for (int i = 0; i < self->workers_count; i++)
	{
		thread_set_affinity(&self->workers[i]->task.thread, cpu);
		cpu++;
	}
}

static inline void
backend_mgr_start(BackendMgr* self, int count)
{
	auto workers = (Backend**)am_malloc(sizeof(Backend*) * count);
	for (auto i = 0; i < count; i++)
		workers[i] = backend_allocate();

	auto on_error = error_catch
	(
		for (auto i = 0; i < count; i++)
			backend_start(workers[i]);
	);
	if (on_error)
	{
		for (auto i = 0; i < count; i++)
		{
			backend_stop(workers[i]);
			backend_free(workers[i]);
		}
		am_free(workers);
		rethrow();
	}
	self->workers       = workers;
	self->workers_count = count;

	// set cpu affinity
	if (opt_int_of(&config()->cpu_affinity))
		backend_mgr_set_affinity(self);
}

static inline void
backend_mgr_stop(BackendMgr* self)
{
	if (self->workers == NULL)
		return;
	for (int i = 0; i < self->workers_count; i++)
	{
		backend_stop(self->workers[i]);
		backend_free(self->workers[i]);
	}
	am_free(self->workers);
	self->workers = NULL;
}

static inline Backend*
backend_mgr_find(BackendMgr* self, Task* task)
{
	for (int i = 0; i < self->workers_count; i++)
	{
		auto backend = self->workers[i];
		if (&backend->task == task)
			return backend;
	}
	return NULL;
}

static inline void
backend_mgr_deploy(BackendMgr* self, PartList* list)
{
	// evenly redistribute backends and create pods
	// for each partition
	auto order = 0;
	list_foreach(&list->list)
	{
		auto part = list_at(Part, link);
		if (order == self->workers_count)
			order = 0;
		auto backend = self->workers[order];
		rpc(&backend->task, MSG_DEPLOY, 1, part);
		order++;
	}
}

static inline void
backend_mgr_undeploy(BackendMgr* self, PartList* list)
{
	// drop pods associated with partitions
	list_foreach(&list->list)
	{
		auto part = list_at(Part, link);
		if (! part->pipeline.backend)
			continue;
		auto backend = backend_mgr_find(self, part->pipeline.backend);
		assert(backend);
		rpc(&backend->task, MSG_UNDEPLOY, 1, part);
	}
}
