
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
#include <amelie_compute.h>

hot static void
compute_main_client(void* arg)
{
	auto client = (Client*)arg;
	auto self   = (Compute*)client->arg;

	clients_add(&self->clients, client);

	// process client connection
	Task* migrate_to = NULL;
	error_catch
	(
		client_attach(client);
		if (! client->accepted)
			client_accept(client);
		migrate_to = compute_link(self, client);
	);

	client_detach(client);
	clients_del(&self->clients, client);

	// migrate client to other compute
	if (migrate_to)
	{
		task_send(migrate_to, &client->msg);
		return;
	}

	client_free(client);
}

static void
compute_rpc(Rpc* rpc, void* arg)
{
	Compute* self = arg;
	switch (rpc->msg.id) {
	case MSG_STOP:
	{
		// disconnect clients
		clients_shutdown(&self->clients);
		break;
	}
	default:
		break;
	}
}

static void
compute_main(void* arg)
{
	Compute* self = arg;
	bool stop = false;
	while (! stop)
	{
		auto msg = task_recv();
		switch (msg->id) {
		case MSG_CLIENT:
		{
			// remote client
			auto client = (Client*)msg;
			client->arg = self;
			coroutine_create(compute_main_client, client);
			break;
		}
		case MSG_RECORD:
		{
			auto record = (RecordMsg*)msg;
			replay_run(&self->replay, record);
			break;
		}
#if 0
		case MSG_STREAM:
		{
			auto stream = (Stream*)msg;
			track_write(&stream->user->track, msg);
			break;
		}
		case MSG_STREAM_CANCEL:
		{
			auto stream = container_of(msg, Stream, msg_cancel);
			track_write(&stream->part->track, msg);
			break;
		}
#endif
		default:
		{
			// command
			stop = msg->id == MSG_STOP;
			auto rpc = rpc_of(msg);
			rpc_execute(rpc, compute_rpc, self);
			break;
		}
		}
	}
}

void
compute_init(Compute* self, ComputeIf* iface, void* iface_arg)
{
	self->iface     = iface;
	self->iface_arg = iface_arg;
	clients_init(&self->clients);
	auth_init(&self->auth);
	replay_init(&self->replay, self);
	task_init(&self->task);
}

void
compute_free(Compute* self)
{
	clients_free(&self->clients);
	replay_free(&self->replay);
	auth_free(&self->auth);
	task_free(&self->task);
}

void
compute_start(Compute* self)
{
	task_create(&self->task, "compute", compute_main, self);
}

void
compute_stop(Compute* self)
{
	// send stop request
	if (task_active(&self->task))
	{
		rpc(&self->task, MSG_STOP, 0);
		task_wait(&self->task);
	}
}
