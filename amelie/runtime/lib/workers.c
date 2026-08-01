
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

void
workers_init(Workers* self)
{
	self->shutdown      = false;
	self->reqs_count    = 0;
	self->workers_count = 0;
	list_init(&self->reqs);
	list_init(&self->workers);
	mutex_init(&self->lock);
	cond_var_init(&self->cond_var);
}

void
workers_free(Workers* self)
{
	mutex_free(&self->lock);
	cond_var_free(&self->cond_var);
}

void
workers_start(Workers* self, int count)
{
	self->shutdown = false;
	for (auto i = count; i > 0; i--)
	{
		auto worker = worker_allocate(self);
		list_append(&self->workers, &worker->link);
		self->workers_count++;
		worker_start(worker);
	}
}

static void
workers_shutdown(Workers* self)
{
	mutex_lock(&self->lock);
	self->shutdown = true;
	cond_var_broadcast(&self->cond_var);
	mutex_unlock(&self->lock);
}

void
workers_stop(Workers* self)
{
	workers_shutdown(self);
	list_foreach_safe(&self->workers)
	{
		auto worker = list_at(Worker, link);
		worker_stop(worker);
		worker_free(worker);
	}
	list_init(&self->workers);
	self->workers_count = 0;
}

void
workers_add(Workers* self, WorkerReq* req)
{
	mutex_lock(&self->lock);
	list_append(&self->reqs, &req->link);
	self->reqs_count++;
	cond_var_signal(&self->cond_var);
	mutex_unlock(&self->lock);
}

WorkerReq*
workers_next(Workers* self)
{
	mutex_lock(&self->lock);

	WorkerReq* req = NULL;
	for (;;)
	{
		if (self->reqs_count > 0)
		{
			req = container_of(list_pop(&self->reqs), WorkerReq, link);
			self->reqs_count--;
			break;
		}

		if (unlikely(self->shutdown))
			break;

		cond_var_wait(&self->cond_var, &self->lock);
	}

	mutex_unlock(&self->lock);
	return req;
}
