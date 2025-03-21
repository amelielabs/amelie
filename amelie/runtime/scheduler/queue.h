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

typedef struct Queue Queue;

struct Queue
{
	bool     lock;
	uint64_t lock_id;
	List     list;
	int      list_count;
};

static inline void
queue_init(Queue* self)
{
	self->lock       = false;
	self->lock_id    = 0;
	self->list_count = 0;
	list_init(&self->list);
}

hot static inline void
queue_add(Queue* self, Job* job)
{
	// jobs are ordered by [id, id_step]
	list_foreach_reverse_safe(&self->list)
	{
		auto ref = list_at(Job, link_queue);
		if (ref->id > job->id)
			continue;

		if (ref->id == job->id) {
			assert(ref->id_step < job->id_step);
		}

		list_insert_after(&ref->link_queue, &job->link_queue);
		self->list_count++;
		return;
	}

	// head
	list_push(&self->list, &job->link_queue);
	self->list_count++;
}

hot static inline Job*
queue_begin(Queue* self)
{
	if (! self->list_count)
		return NULL;

	// queue is locked (first job is active)
	if (self->lock)
		return NULL;

	auto job = container_of(list_first(&self->list), Job, link_queue);
	if (self->lock_id > 0)
	{
		// previously completed job was not last, next job must
		// have the same lock id
		if (job->id != self->lock_id)
			return NULL;
	} else {
		self->lock_id = job->id;
	}

	self->lock = true;
	return job;
}

hot static inline void
queue_end(Queue* self, Job* job)
{
	// completed job is always first
	assert(self->list_count);
	assert(container_of(list_first(&self->list), Job, link_queue) == job);
	assert(self->lock);
	assert(self->lock_id == job->id);

	// unlock queue and reset lock id, if the job was last
	self->lock = false;
	if (job->last)
		self->lock_id = 0;

	list_pop(&self->list);
	self->list_count--;
}
