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
	int      list_count;
	List     list;
};

static inline void
job_cache_init(JobCache* self)
{
	self->list_count = 0;
	spinlock_init(&self->lock);
	list_init(&self->list);
}

static inline void
job_cache_free(JobCache* self)
{
	list_foreach_safe(&self->list)
	{
		auto job = list_at(Job, link);
		job_free_memory(job);
	}
	list_init(&self->list);
	spinlock_free(&self->lock);
}

static inline Job*
job_cache_pop(JobCache* self)
{
	spinlock_lock(&self->lock);
	Job* job = NULL;
	if (likely(self->list_count > 0))
	{
		auto first = list_pop(&self->list);
		job = container_of(first, Job, link);
		self->list_count--;
	}
	spinlock_unlock(&self->lock);
	return job;
}

static inline void
job_cache_push(JobCache* self, Job* job)
{
	job_reset(job);
	spinlock_lock(&self->lock);
	list_append(&self->list, &job->link);
	self->list_count++;
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
