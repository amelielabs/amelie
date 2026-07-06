
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
#include <amelie_repl.h>

static void
receiver_main(void* arg)
{
	Rpc*      rpc    = arg;
	Client*   client = rpc->arg;
	Receiver* self   = client->arg;
	defer(rpc_signal, rpc);

	client_attach(client);

	Node node;
	node_init(&node, self->db,
	          self->recover_if,
	          self->recover_if_arg,
	          client);

	list_append(&self->list, &node.link);
	self->list_count++;

	error_catch
	(
		node_main(&node);
	);

	list_unlink(&node.link);
	self->list_count--;

	am_self()->cancel = false;
	node_free(&node);
}

static void
receiver_shutdown(Rpc* rpc, void* arg)
{
	unused(rpc);
	Receiver* self = arg;
	while (self->list_count > 0)
	{
		auto node = container_of(self->list.next, Node, link);
		coroutine_kill(node->coroutine_id);
	}
}

static void
receiver_task_main(void* arg)
{
	Receiver* self = arg;
	for (;;)
	{
		auto msg = task_recv();
		auto rpc = rpc_of(msg);

		if (msg->id == MSG_STOP)
		{
			rpc_execute(rpc, receiver_shutdown, self);
			return;
		}

		Client* client = rpc->arg;
		client->arg = self;
		coroutine_create(receiver_main, rpc);
	}
}

void
receiver_init(Receiver* self, Db* db, RecoverIf* iface, void* iface_arg)
{
	self->list_count     = 0;
	self->recover_if     = iface;
	self->recover_if_arg = iface_arg;
	self->db             = db;
	task_init(&self->task);
	list_init(&self->list);
}

void
receiver_free(Receiver* self)
{
	task_free(&self->task);
}

void
receiver_start(Receiver* self)
{
	task_create(&self->task, "receiver", receiver_task_main, self);
}

void
receiver_stop(Receiver* self)
{
	rpc(&self->task, MSG_STOP, 0);
	task_wait(&self->task);
	task_free(&self->task);
	task_init(&self->task);
}

void
receiver_send(Receiver* self, Client* client)
{
	rpc(&self->task, MSG_CLIENT, client);
}
