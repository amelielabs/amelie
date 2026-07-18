
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
#include <amelie_repl>
#include <amelie_vm>
#include <amelie_frontend.h>
#include <amelie_backend.h>

static void
backend_rpc(Rpc* rpc, void* arg)
{
	Backend* self = arg;
	switch (rpc->msg.id) {
	case MSG_DEPLOY:
	{
		// load partition heap file
		Part* part = rpc->arg;
		if (opt_int_of(&state()->recover) == RECOVER_CHECKPOINT)
			part_open(part, state_checkpoint());

		// create and start new pod
		pods_create(&self->pods, part);
		break;
	}
	case MSG_UNDEPLOY:
	{
		// stop and drop pod
		Part* part = rpc->arg;
		pods_drop_by(&self->pods, part);
		break;
	}
	case MSG_CLEANUP:
	{
		PartCleanup* cleanup = rpc->arg;
		defer(part_cleanup_free, cleanup);
		part_cleanup_run(cleanup);
		break;
	}
	case MSG_STOP:
	{
		// shutdown pods
		pods_shutdown(&self->pods);
		break;
	}
	default:
		abort();
		break;
	}
}

hot static void
backend_main(void* arg)
{
	Backend* self = arg;
	bool stop = false;
	while (! stop)
	{
		auto msg = task_recv();
		switch (msg->id) {
		// forward messages to the pods
		case MSG_LTR:
		{
			auto ltr = (Ltr*)msg;
			track_write(&ltr->part->track, msg);
			break;
		}
		case MSG_LTR_STOP:
		{
			auto ltr = container_of(msg, Ltr, queue_close);
			ltr_write(ltr, msg);
			break;
		}
		case MSG_REQ:
		{
			auto req = (Req*)msg;
			ltr_write(req->ltr, msg);
			break;
		}
		default:
			// rpc commands
			stop = msg->id == MSG_STOP;
			auto rpc = rpc_of(msg);
			rpc_execute(rpc, backend_rpc, self);
			break;
		}
	}
}

Backend*
backend_allocate(void)
{
	auto self = (Backend*)am_malloc(sizeof(Backend));
	pods_init(&self->pods, &self->task);
	task_init(&self->task);
	list_init(&self->link);
	return self;
}

void
backend_free(Backend* self)
{
	task_free(&self->task);
	am_free(self);
}

void
backend_start(Backend* self)
{
	task_create(&self->task, "backend", backend_main, self);
}

void
backend_stop(Backend* self)
{
	// send stop request
	if (task_active(&self->task))
	{
		rpc(&self->task, MSG_STOP, NULL);
		task_wait(&self->task);
	}
}
