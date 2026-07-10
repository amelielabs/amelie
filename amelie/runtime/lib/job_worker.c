
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
#include <amelie_io.h>
#include <amelie_lib.h>

static void
job_worker_main(void* arg)
{
	auto self = (JobWorker*)arg;
	for (;;)
	{
		auto job = jobs_next(self->jobs);
		if (! job)
			break;
		job_run(job);
	}
}

JobWorker*
job_worker_allocate(Jobs* jobs)
{
	auto self = (JobWorker*)am_malloc(sizeof(JobWorker));
	self->jobs = jobs;
	task_init(&self->task);
	list_init(&self->link);
	return self;
}

void
job_worker_free(JobWorker* self)
{
	task_free(&self->task);
	am_free(self);
}

void
job_worker_start(JobWorker* self)
{
	task_create(&self->task, "job_worker", job_worker_main, self);
}

void
job_worker_stop(JobWorker* self)
{
	task_wait(&self->task);
}
