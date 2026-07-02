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

typedef struct Frontends Frontends;

struct Frontends
{
	int       rr;
	int       workers_count;
	Frontend* workers;
};

static inline void
frontends_init(Frontends* self)
{
	self->rr            = 0;
	self->workers_count = 0;
	self->workers       = NULL;
}

static inline void
frontends_set_affinity(Frontends* self)
{
	auto cpus = get_nprocs();
	if (self->workers_count > cpus)
	{
		info("cpu_affinity: the total number of frontend workers is more then "
		     "the number of available cpu cores, skipping.");
		return;
	}
	for (int i = 0; i < self->workers_count; i++)
		thread_set_affinity(&self->workers[i].task.thread, i);
}

static inline void
frontends_start(Frontends*  self,
                FrontendIf* iface,
                void*       iface_arg,
                int         count)
{
	if (count == 0)
		return;
	self->workers_count = count;
	self->workers = am_malloc(sizeof(Frontend) * count);
	int i = 0;
	for (; i < count; i++)
		frontend_init(&self->workers[i], iface, iface_arg);
	for (i = 0; i < count; i++)
		frontend_start(&self->workers[i]);

	// set cpu affinity
	if (opt_int_of(&config()->cpu_affinity))
		frontends_set_affinity(self);
}

static inline void
frontends_stop(Frontends* self)
{
	if (self->workers == NULL)
		return;
	for (int i = 0; i < self->workers_count; i++)
	{
		frontend_stop(&self->workers[i]);
		frontend_free(&self->workers[i]);
	}
	am_free(self->workers);
	self->workers = NULL;
}

static inline int
frontends_next(Frontends* self)
{
	assert(self->workers_count > 0);
	if (self->rr == self->workers_count)
		self->rr = 0;
	return self->rr++;
}

static inline void
frontends_forward(Frontends* self, Msg* msg)
{
	assert(self->workers_count > 0);
	int pos = frontends_next(self);
	frontend_add(&self->workers[pos], msg);
}

static inline void
frontends_invalidate(Frontends* self, User* user)
{
	if (self->workers == NULL)
		return;
	for (int i = 0; i < self->workers_count; i++)
	{
		auto frontend = &self->workers[i];
		auth_cache_invalidate(&frontend->auth.cache, user);
	}
}

static inline void
frontends_invalidate_all(Frontends* self)
{
	if (self->workers == NULL)
		return;
	for (int i = 0; i < self->workers_count; i++)
	{
		auto frontend = &self->workers[i];
		auth_reset(&frontend->auth);
	}
}
