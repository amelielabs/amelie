
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
worker_main(void* arg)
{
	auto self = (Worker*)arg;
	for (;;)
	{
		auto req = workers_next(self->workers);
		if (! req)
			break;
		worker_req_run(req);
	}
}

Worker*
worker_allocate(Workers* workers)
{
	auto self = (Worker*)am_malloc(sizeof(Worker));
	self->workers = workers;
	task_init(&self->task);
	list_init(&self->link);
	return self;
}

void
worker_free(Worker* self)
{
	task_free(&self->task);
	am_free(self);
}

void
worker_start(Worker* self)
{
	task_create(&self->task, "worker", worker_main, self);
}

void
worker_stop(Worker* self)
{
	task_wait(&self->task);
}
