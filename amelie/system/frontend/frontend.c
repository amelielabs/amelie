
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

hot static void
frontend_main_client(void* arg)
{
	auto client = (Client*)arg;
	auto self   = (Frontend*)client->arg;

	clients_add(&self->clients, client);

	// process client connection
	error_catch
	(
		client_attach(client);
		client_accept(client);
		frontend_client(self, client);
	);

	clients_del(&self->clients, client);

	client_close(client);
	client_free(client);
}

hot static void
frontend_main_player(void* arg)
{
	auto player = (Player*)arg;

	// process recovery connection
	player_main(player);
}

static void
frontend_rpc(Rpc* rpc, void* arg)
{
	Frontend* self = arg;
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
frontend_main(void* arg)
{
	Frontend* self = arg;
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
			coroutine_create(frontend_main_client, client);
			break;
		}
		case MSG_PLAYER:
		{
			// player
			auto player = (Player*)msg;
			player->arg = self;
			coroutine_create(frontend_main_player, player);
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
		default:
		{
			// command
			stop = msg->id == MSG_STOP;
			auto rpc = rpc_of(msg);
			rpc_execute(rpc, frontend_rpc, self);
			break;
		}
		}
	}
}

void
frontend_init(Frontend*   self,
              FrontendIf* iface,
              void*       iface_arg)
{
	self->iface     = iface;
	self->iface_arg = iface_arg;
	clients_init(&self->clients);
	auth_init(&self->auth);
	task_init(&self->task);
}

void
frontend_free(Frontend* self)
{
	clients_free(&self->clients);
	auth_free(&self->auth);
	task_free(&self->task);
}

void
frontend_start(Frontend* self)
{
	task_create(&self->task, "frontend", frontend_main, self);
}

void
frontend_stop(Frontend* self)
{
	// send stop request
	if (task_active(&self->task))
	{
		rpc(&self->task, MSG_STOP, 0);
		task_wait(&self->task);
	}
}
