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

typedef struct JobMgr JobMgr;

struct JobMgr
{
	List        pending;
	int         pending_count;
	Hashtable   ht;
	JobCacheMgr cache_mgr;
};

static inline void
job_mgr_init(JobMgr* self)
{
	self->pending_count = 0;
	list_init(&self->pending);
	hashtable_init(&self->ht);
	job_cache_mgr_init(&self->cache_mgr);
}

static inline void
job_mgr_free(JobMgr* self)
{
	hashtable_free(&self->ht);
	job_cache_mgr_free(&self->cache_mgr);
}

hot static inline bool
job_mgr_cmp(HashtableNode* node, void* ptr)
{
	auto job = container_of(node, Job, node);
	return job->id == *(uint64_t*)ptr;
}

hot static inline bool
job_mgr_add(JobMgr* self, Job* job)
{
	if  (job->id == UINT64_MAX)
		goto add;

	if (unlikely(! hashtable_created(&self->ht)))
		hashtable_create(&self->ht, 128);
	hashtable_reserve(&self->ht);

	// find or create
	job->node.hash = hash_murmur3_32((uint8_t*)&job->id, sizeof(job->id), 0);

	auto node = hashtable_get(&self->ht, job->node.hash, job_mgr_cmp, &job->id);
	if (node)
	{
		// put job to the wait queue
		auto head = container_of(node, Job, node);
		if (head->queue)
			head->queue_tail->next = job;
		else
			head->queue = job;
		head->queue_tail = job;
		return false;
	}

	// add to the hash table and schedule for execution
	hashtable_set(&self->ht, &job->node);

add:
	list_append(&self->pending, &job->link_mgr);
	self->pending_count++;
	return true;
}

hot static inline Job*
job_mgr_begin(JobMgr* self)
{
	if (! self->pending_count)
		return NULL;

	// take next pending job, job remains in the hash table
	// (if it was added there)
	auto first = list_pop(&self->pending);
	self->pending_count--;
	auto job = container_of(first, Job, node);
	return job;
}

hot static inline bool
job_mgr_end(JobMgr* self, Job* job)
{
	if (job->id == UINT64_MAX)
	{
		// job without dispatch (like commit/abort)
		job_free(job);
		return false;
	}

	// completed job is always first
	hashtable_delete(&self->ht, &job->node);
	if (! job->next)
		return false;

	// use next job in the queue
	auto next = job->next;
	next->queue = next;
	next->queue_tail = job->queue_tail;

	// add to the hash table and schedule for execution
	hashtable_set(&self->ht, &next->node);

	list_append(&self->pending, &next->link_mgr);
	self->pending_count++;
	return true;
}
