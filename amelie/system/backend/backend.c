
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
#include <amelie_engine>
#include <amelie_storage>
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
		// create and start new pod
		Part* part = rpc_arg_ptr(rpc, 0);
		pod_mgr_create(&self->pod_mgr, part);
		break;
	}
	case MSG_UNDEPLOY:
	{
		// stop and drop pod
		Part* part = rpc_arg_ptr(rpc, 0);
		pod_mgr_drop_by(&self->pod_mgr, part);
		break;
	}
	case MSG_STOP:
	{
		// shutdown pods
		pod_mgr_shutdown(&self->pod_mgr);
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
	pod_mgr_init(&self->pod_mgr, &self->task);
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
		rpc(&self->task, MSG_STOP, 0);
		task_wait(&self->task);
	}
}
