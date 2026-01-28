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
	int       rr;
};

static inline void
backend_mgr_init(BackendMgr* self)
{
	self->workers       = NULL;
	self->workers_count = 0;
	self->rr            = 0;
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

static inline Backend*
backend_mgr_next(BackendMgr* self)
{
	if (self->rr == self->workers_count)
		self->rr = 0;
	auto order = self->rr++;
	return self->workers[order];
}

static inline void
backend_mgr_deploy(BackendMgr* self, Part* part)
{
	auto backend = backend_mgr_next(self);
	rpc(&backend->task, MSG_DEPLOY, part);
}

static inline void
backend_mgr_deploy_all(BackendMgr* self, PartMgr* part_mgr)
{
	RpcSet set;
	rpc_set_init(&set);
	defer(rpc_set_free, &set);
	rpc_set_prepare(&set, part_mgr->parts_count);

	auto order = 0;
	list_foreach(&part_mgr->parts)
	{
		auto part = list_at(Part, link);
		auto rpc = rpc_set_add(&set, order, MSG_DEPLOY, part);
		auto backend = backend_mgr_next(self);
		rpc_send(rpc, &backend->task);
		order++;
	}
	rpc_set_wait(&set);
}

static inline void
backend_mgr_undeploy(BackendMgr* self, Part* part)
{
	if (! part->track.backend)
		return;
	auto backend = backend_mgr_find(self, part->track.backend);
	assert(backend);
	rpc(&backend->task, MSG_UNDEPLOY, part);
}

static inline void
backend_mgr_undeploy_all(BackendMgr* self, PartMgr* part_mgr)
{
	RpcSet set;
	rpc_set_init(&set);
	defer(rpc_set_free, &set);
	rpc_set_prepare(&set, part_mgr->parts_count);

	auto order = 0;
	list_foreach(&part_mgr->parts)
	{
		auto part = list_at(Part, link);
		auto rpc = rpc_set_add(&set, order, MSG_UNDEPLOY, part);
		auto backend = backend_mgr_find(self, part->track.backend);
		rpc_send(rpc, &backend->task);
		order++;
	}
	rpc_set_wait(&set);
}
