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

typedef struct Computes Computes;

struct Computes
{
	int      rr;
	int      workers_count;
	Compute* workers;
};

static inline void
computes_init(Computes* self)
{
	self->rr            = 0;
	self->workers_count = 0;
	self->workers       = NULL;
}

static inline void
computes_set_affinity(Computes* self)
{
	auto cpus = get_nprocs();
	if (self->workers_count > cpus)
	{
		info("cpu_affinity: the total number of compute workers is more then "
		     "the number of available cpu cores, skipping.");
		return;
	}
	for (int i = 0; i < self->workers_count; i++)
		thread_set_affinity(&self->workers[i].task.thread, i);
}

static inline void
computes_start(Computes*  self,
                ComputeIf* iface,
                void*       iface_arg,
                int         count)
{
	if (count == 0)
		return;
	self->workers_count = count;
	self->workers = am_malloc(sizeof(Compute) * count);
	int i = 0;
	for (; i < count; i++)
		compute_init(&self->workers[i], iface, iface_arg);
	for (i = 0; i < count; i++)
		compute_start(&self->workers[i]);

	// set cpu affinity
	if (opt_int_of(&config()->cpu_affinity))
		computes_set_affinity(self);
}

static inline void
computes_stop(Computes* self)
{
	if (self->workers == NULL)
		return;
	for (int i = 0; i < self->workers_count; i++)
	{
		compute_stop(&self->workers[i]);
		compute_free(&self->workers[i]);
	}
	am_free(self->workers);
	self->workers = NULL;
}

static inline int
computes_next(Computes* self)
{
	assert(self->workers_count > 0);
	if (self->rr == self->workers_count)
		self->rr = 0;
	return self->rr++;
}

static inline void
computes_forward(Computes* self, Msg* msg)
{
	assert(self->workers_count > 0);
	int pos = computes_next(self);
	compute_add(&self->workers[pos], msg);
}

static inline void
computes_invalidate(Computes* self, User* user)
{
	if (self->workers == NULL)
		return;
	for (int i = 0; i < self->workers_count; i++)
	{
		auto compute = &self->workers[i];
		auth_cache_invalidate(&compute->auth.cache, user);
	}
}

static inline void
computes_invalidate_all(Computes* self)
{
	if (self->workers == NULL)
		return;
	for (int i = 0; i < self->workers_count; i++)
	{
		auto compute = &self->workers[i];
		auth_reset(&compute->auth);
	}
}

static inline void
computes_deploy(Computes* self, User* user)
{
	auto id = computes_next(self);
	auto compute = &self->workers[id];
	track_set_compute(&user->track, &compute->task);
}

static inline void
computes_undeploy(Computes* self, User* user)
{
	unused(self);
	track_set_compute(&user->track, NULL);
}
