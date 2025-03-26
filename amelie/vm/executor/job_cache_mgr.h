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

typedef struct JobCacheMgr JobCacheMgr;

struct JobCacheMgr
{
	atomic_u64 seq;
	JobCache   cache[8];
};

static inline void
job_cache_mgr_init(JobCacheMgr* self)
{
	self->seq = 0;
	for (int i = 0; i < 8; i++)
		job_cache_init(&self->cache[i]);
}

static inline void
job_cache_mgr_free(JobCacheMgr* self)
{
	self->seq = 0;
	for (int i = 0; i < 8; i++)
		job_cache_free(&self->cache[i]);
}

static inline Job*
job_create(JobCacheMgr* self)
{
	int next = atomic_u64_inc(&self->seq) % 8;
	return job_cache_create(&self->cache[next]);
}

static inline void
job_free(Job* self)
{
	if (likely(self->cache))
	{
		job_cache_push(self->cache, self);
		return;
	}
	job_free_memory(self);
}
