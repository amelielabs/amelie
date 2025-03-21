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

typedef struct Scheduler Scheduler;

struct Scheduler
{
	Mutex    lock;
	CondVar  cond_var;
	uint64_t seq;
	List     list;
	int      list_count;
};

static inline void
scheduler_init(Scheduler* self)
{
	self->seq        = 0;
	self->list_count = 0;
	mutex_init(&self->lock);
	cond_var_init(&self->cond_var);
	list_init(&self->list);
}

static inline void
scheduler_free(Scheduler* self)
{
	mutex_free(&self->lock);
	cond_var_free(&self->cond_var);
}

hot static inline bool
scheduler_next(Scheduler* self, Queue* queue)
{
	auto job = queue_begin(queue);
	if (! job)
		return false;

	// add job to the execution list
	list_append(&self->list, &job->link_scheduler);
	self->list_count++;
	return true;
}

hot static inline void
scheduler_add(Scheduler* self, Dispatch* dispatch, int step, JobList* list)
{
	mutex_lock(&self->lock);

	// assign job/dispatch id on first step
	uint64_t id;
	if (step == 0)
	{
		id = ++self->seq;
		if (dispatch)
			dispatch->id = id;
	} else {
		assert(dispatch);
		id = dispatch->id;
	}

	bool wakeup = false;
	list_foreach_safe(&list->list)
	{
		auto job = list_at(Job, link);

		// move job to the dispatch step list
		if (dispatch)
		{
			job_list_del(list, job);
			dispatch_add(dispatch, step, job);
		} else {
			job->id = id;
		}

		// add job to the queue (ordered by id)
		queue_add(job->queue, job);

		// schedule next job from the queue (if any)
		if (scheduler_next(self, job->queue))
			wakeup = true;
	}

	// wakeup next worker
	if (wakeup)
		cond_var_signal(&self->cond_var);

	mutex_unlock(&self->lock);
}

hot static inline Job*
scheduler_begin(Scheduler* self)
{
	mutex_lock(&self->lock);

	// todo: shutdown

	// wait for next available job
	while (! self->list_count)
		cond_var_wait(&self->cond_var, &self->lock);

	// remove job from the scheduler list (job remains first in the queue)
	auto job = container_of(list_pop(&self->list), Job, link_scheduler);
	self->list_count--;

	mutex_unlock(&self->lock);
	return job;
}

hot static inline void
scheduler_end(Scheduler* self, Job* job)
{
	// reply job completion status to the dispatch step channel
	if (job->dispatch)
	{
		int id;
		if (job->error)
			id = MSG_ERROR;
		else
			id = MSG_OK;
		auto buf = msg_create(id);
		msg_end(buf);
		auto step = dispatch_at(job->dispatch, job->id_step);
		channel_write(&step->src, buf);
	}

	mutex_lock(&self->lock);

	// remove job from the queue (always first)
	queue_end(job->queue, job);

	// schedule next job from the queue (if any)
	auto wakeup = scheduler_next(self, job->queue);
	mutex_unlock(&self->lock);

	if (wakeup)
		cond_var_signal(&self->cond_var);

	// free job if it has no dispatch
	if (! job->dispatch)
		job_free(job);
}
