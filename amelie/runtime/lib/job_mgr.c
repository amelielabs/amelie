
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

#include <amelie_base.h>
#include <amelie_os.h>
#include <amelie_lib.h>

void
job_mgr_init(JobMgr* self)
{
	self->shutdown = false;
	self->jobs_count = 0;
	self->workers_count = 0;
	list_init(&self->jobs);
	list_init(&self->workers);
	mutex_init(&self->lock);
	cond_var_init(&self->cond_var);
}

void
job_mgr_free(JobMgr* self)
{
	mutex_free(&self->lock);
	cond_var_free(&self->cond_var);
}

void
job_mgr_start(JobMgr* self, int count)
{
	self->shutdown = false;
	for (auto i = count; i > 0; i--)
	{
		auto worker = job_worker_allocate(self);
		list_append(&self->workers, &worker->link);
		self->workers_count++;
		job_worker_start(worker);
	}
}

static void
job_mgr_shutdown(JobMgr* self)
{
	mutex_lock(&self->lock);
	self->shutdown = true;
	cond_var_broadcast(&self->cond_var);
	mutex_unlock(&self->lock);
}

void
job_mgr_stop(JobMgr* self)
{
	job_mgr_shutdown(self);
	list_foreach_safe(&self->workers)
	{
		auto worker = list_at(JobWorker, link);
		job_worker_stop(worker);
		job_worker_free(worker);
	}
	list_init(&self->workers);
	self->workers_count = 0;
}

void
job_mgr_add(JobMgr* self, Job* job)
{
	mutex_lock(&self->lock);
	list_append(&self->jobs, &job->link);
	self->jobs_count++;
	cond_var_signal(&self->cond_var);
	mutex_unlock(&self->lock);
}

Job*
job_mgr_next(JobMgr* self)
{
	mutex_lock(&self->lock);

	Job* job = NULL;
	for (;;)
	{
		if (self->jobs_count > 0)
		{
			job = container_of(list_pop(&self->jobs), Job, link);
			self->jobs_count--;
			break;
		}

		if (unlikely(self->shutdown))
			break;

		cond_var_wait(&self->cond_var, &self->lock);
	}

	mutex_unlock(&self->lock);
	return job;
}
