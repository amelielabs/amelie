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

typedef struct Backends Backends;

struct Backends
{
	Backend** workers;
	int       workers_count;
	int       rr;
};

static inline void
backends_init(Backends* self)
{
	self->workers       = NULL;
	self->workers_count = 0;
	self->rr            = 0;
}

static inline void
backends_set_affinity(Backends* self)
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
backends_start(Backends* self, int count)
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
		backends_set_affinity(self);
}

static inline void
backends_stop(Backends* self)
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
backends_find(Backends* self, Task* task)
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
backends_next(Backends* self)
{
	if (self->rr == self->workers_count)
		self->rr = 0;
	auto order = self->rr++;
	return self->workers[order];
}

static inline void
backends_deploy(Backends* self, User* user)
{
	auto backend = backends_next(self);
	rpc(&backend->task, MSG_DEPLOY, user);
}

static inline void
backends_undeploy(Backends* self, User* user)
{
	unused(self);
	if (! user->track.backend)
		return;
	rpc(user->track.backend, MSG_UNDEPLOY, user);
}

static inline void
backends_sync(Backends* self, User* user)
{
	unused(self);
	rpc(user->track.backend, MSG_SYNC, user);
}

static inline void
backends_truncate(Backends* self, Table* table)
{
	unused(self);
	rpc(table->user->track.backend, MSG_TRUNCATE, table);
}
