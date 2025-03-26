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

typedef struct JobCache JobCache;

struct JobCache
{
	Spinlock lock;
	JobList  list;
};

static inline void
job_cache_init(JobCache* self)
{
	spinlock_init(&self->lock);
	job_list_init(&self->list);
}

static inline void
job_cache_free(JobCache* self)
{
	list_foreach_safe(&self->list.list)
	{
		auto job = list_at(Job, link);
		job_free_memory(job);
	}
	job_list_init(&self->list);
	spinlock_free(&self->lock);
}

static inline Job*
job_cache_pop(JobCache* self)
{
	spinlock_lock(&self->lock);
	auto job = job_list_pop(&self->list);
	spinlock_unlock(&self->lock);
	return job;
}

static inline void
job_cache_push(JobCache* self, Job* job)
{
	job_reset(job);
	spinlock_lock(&self->lock);
	job_list_add(&self->list, job);
	spinlock_unlock(&self->lock);
}

hot static inline Job*
job_cache_create(JobCache* self)
{
	auto job = job_cache_pop(self);
	if (job) {
		list_init(&job->link);
	} else
	{
		job = job_allocate();
		job->cache = self;
	}
	return job;
}
