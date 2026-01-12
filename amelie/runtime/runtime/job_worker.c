
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
#include <amelie_json.h>
#include <amelie_runtime.h>

static void
job_worker_main(void* arg)
{
	auto self = (JobWorker*)arg;
	for (;;)
	{
		auto job = job_mgr_next(self->job_mgr);
		if (! job)
			break;
		job_run(job);
	}
}

JobWorker*
job_worker_allocate(JobMgr* job_mgr)
{
	auto self = (JobWorker*)am_malloc(sizeof(JobWorker));
	self->job_mgr = job_mgr;
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
