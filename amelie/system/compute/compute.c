
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
	error_catch
	(
		client_attach(client);
		client_accept(client);
		compute_link(self, client);
	);

	clients_del(&self->clients, client);

	client_close(client);
	client_free(client);
}

hot static void
compute_main_player(void* arg)
{
	auto player = (Player*)arg;

	// process recovery connection
	player_main(player);
}

static void
compute_rpc(Rpc* rpc, void* arg)
{
	Compute* self = arg;
	switch (rpc->msg.id) {
	case MSG_DEPLOY:
	{
		// create and start new pod
		User* user = rpc->arg;
		pods_create(&self->pods, user);
		break;
	}
	case MSG_UNDEPLOY:
	{
		// stop and drop pod
		User* user = rpc->arg;
		pods_drop_by(&self->pods, user);
		break;
	}
	case MSG_SYNC:
	{
		// commit prepared transactions
		User* user = rpc->arg;
		auto pod = pods_find(&self->pods, user);
		pod_sync(pod);
		break;
	}
	case MSG_TRUNCATE:
	{
		// truncate partition
		Table* table = rpc->arg;
		// todo: cancel all pending streams
			// streaming_cancel(part);
		part_truncate(table->part);
		break;
	}
	case MSG_STOP:
	{
		// disconnect clients
		clients_shutdown(&self->clients);

		// shutdown pods
		pods_shutdown(&self->pods);
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
		case MSG_PLAYER:
		{
			// player
			auto player = (Player*)msg;
			player->arg = self;
			coroutine_create(compute_main_player, player);
			break;
		}
		case MSG_PLAYER_STOP:
		{
			auto player = container_of(msg, Player, msg_stop);
			player_forward(player, msg);
			break;
		}
		case MSG_RECORD:
		{
			// forward for execution
			auto record = (RecordMsg*)msg;
			player_forward(record->arg, msg);
			break;
		}

		// --------------------------------------

#if 0
		// forward messages to the pods
		case MSG_LTR:
		{
			auto ltr = (Ltr*)msg;
			track_write(&ltr->user->track, msg);
			break;
		}
		case MSG_LTR_STOP:
		{
			auto ltr = container_of(msg, Ltr, queue_close);
			ltr_write(ltr, msg);
			break;
		}
#endif

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

		// --------------------------------------

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
compute_init(Compute*   self,
              ComputeIf* iface,
              void*       iface_arg)
{
	self->iface     = iface;
	self->iface_arg = iface_arg;
	clients_init(&self->clients);
	pods_init(&self->pods, &self->task);
	auth_init(&self->auth);
	task_init(&self->task);
}

void
compute_free(Compute* self)
{
	clients_free(&self->clients);
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
