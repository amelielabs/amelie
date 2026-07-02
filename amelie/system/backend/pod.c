
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

#include <amelie_runtime>
#include <amelie_server>
#include <amelie_db>
#include <amelie_sync>
#include <amelie_vm>
#include <amelie_frontend.h>
#include <amelie_backend.h>

hot static void
pod_request(Pod* self, Ltr* ltr, Req* req)
{
	auto gtr = ltr->gtr;
	switch (req->type) {
	case REQ_EXECUTE:
	{
		// create and add transaction to the prepared list (even on error)
		if (ltr->tr == NULL)
		{
			auto track = self->track;
			auto tr = tr_create(&track->cache);
			tr_set_id(tr, gtr->id);
			tr_set_limit(tr, &gtr->limit);
			tr_list_add(&track->prepared, tr);
			ltr->tr = tr;
		}

		// execute request
		vm_reset(&self->vm);
		reg_prepare(&self->vm.r, req->code->regs);

		Return ret;
		return_init(&ret);

		vm_run(&self->vm, gtr->local,
		        gtr,
		        ltr->tr,
		        NULL,
		        req->code,
		        req->code_data,
		       &req->arg,
		       (Value*)req->refs.start,
		        NULL,
		       &ret,
		        false,
		        req->start);

		if (ret.value)
			value_move(&req->result, ret.value);
		break;
	}
	}
}

hot static void
pod_run(Pod* self, Ltr* ltr)
{
	// execute incoming requests till close
	auto active = true;
	while (active)
	{
		auto msg = ltr_read(ltr);
		if (msg->id == MSG_LTR_STOP)
		{
			active = false;
			continue;
		}
		auto req = (Req*)msg;
		if (error_catch(pod_request(self, ltr, req)))
		{
			req->error = error_create(&am_self()->error);
			if (! ltr->error)
				ltr->error = req->error;
		}
		active = !req->dispatch->close;
		dispatch_complete(req->dispatch);
	}

	calls_reset(&self->vm.calls);
	ltr_complete(ltr);
}

static void
pod_main(void* arg)
{
	Pod* self = arg;
	auto track = self->track;
	for (;;)
	{
		auto msg = track_read(track);
		if (msg->id == MSG_STOP)
			break;
		auto ltr = (Ltr*)msg;

		// abort and commit previously prepared transactions
		track_sync(self->track, &ltr->consensus);

		// execute transaction
		pod_run(self, ltr);
	}
}

Pod*
pod_allocate(Part* part)
{
	auto self = (Pod*)am_malloc(sizeof(Pod));
	self->part      =  part;
	self->track     = &part->track;
	self->worker_id = -1;
	vm_init(&self->vm, part);
	list_init(&self->link);
	return self;
}

void
pod_free(Pod* self)
{
	vm_free(&self->vm);
	am_free(self);
}

void
pod_start(Pod* self, Task* task)
{
	if (self->worker_id != -1)
		return;
	track_set_backend(self->track, task);
	self->worker_id = coroutine_create(pod_main, self);
}

void
pod_stop(Pod* self)
{
	if (self->worker_id == -1)
		return;
	auto worker = coroutines_find(&am_task->coroutines, self->worker_id);
	assert(worker);
	Msg stop;
	msg_init(&stop, MSG_STOP);
	track_write(self->track, &stop);
	wait_event(&worker->on_exit, am_self());
	track_set_backend(self->track, NULL);
	self->worker_id = -1;
}
