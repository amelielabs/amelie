
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

#include <amelie_runtime.h>
#include <amelie_os.h>
#include <amelie_lib.h>
#include <amelie_json.h>
#include <amelie_config.h>
#include <amelie_user.h>
#include <amelie_auth.h>
#include <amelie_http.h>
#include <amelie_client.h>
#include <amelie_server.h>
#include <amelie_io.h>

hot static void
client_main(void* arg)
{
	auto client = (Client*)arg;
	auto self   = (Io*)client->arg;

	client_mgr_add(&self->client_mgr, client);

	// process client connection
	error_catch
	(
		client_attach(client);
		client_accept(client);
		self->on_connect(self, client);
	);

	client_mgr_del(&self->client_mgr, client);

	client_close(client);
	client_free(client);
}

static void
io_rpc(Rpc* rpc, void* arg)
{
	Io* self = arg;
	switch (rpc->id) {
	case RPC_SYNC_USERS:
	{
		// sync user caches
		UserCache* with = rpc_arg_ptr(rpc, 0);
		auth_sync(&self->auth, with);
		break;
	}
	case RPC_LOCK:
		// exclusive lock
		lock_mgr_lock(&self->lock_mgr, LOCK_EXCLUSIVE);
		break;
	case RPC_UNLOCK:
		// exclusive unlock
		lock_mgr_unlock(&self->lock_mgr, LOCK_EXCLUSIVE, NULL);
		break;
	case RPC_STOP:
	{
		// disconnect clients
		client_mgr_shutdown(&self->client_mgr);
		break;
	}
	default:
		break;
	}
}

static void
io_main(void* arg)
{
	Io* self = arg;
	bool stop = false;
	while (! stop)
	{
		auto buf = channel_read(&am_task->channel, -1);
		auto msg = msg_of(buf);
		defer_buf(buf);
		switch (msg->id) {
		case MSG_CLIENT:
		{
			// remote client
			Client* client = *(void**)msg->data;
			client->arg = self;
			coroutine_create(client_main, client);
			break;
		}
		default:
		{
			// command
			stop = msg->id == RPC_STOP;
			auto rpc = rpc_of(buf);
			rpc_execute(rpc, io_rpc, self);
			break;
		}
		}
	}
}

void
io_init(Io*     self,
        IoEvent on_connect,
        void*   on_connect_arg)
{
	self->on_connect     = on_connect;
	self->on_connect_arg = on_connect_arg;
	lock_mgr_init(&self->lock_mgr);
	client_mgr_init(&self->client_mgr);
	auth_init(&self->auth);
	task_init(&self->task);
}

void
io_free(Io* self)
{
	client_mgr_free(&self->client_mgr);
	lock_mgr_free(&self->lock_mgr);
	auth_free(&self->auth);
}

void
io_start(Io* self)
{
	task_create(&self->task, "io", io_main, self);
}

void
io_stop(Io* self)
{
	// send stop request
	if (task_active(&self->task))
	{
		rpc(&self->task.channel, RPC_STOP, 0);
		task_wait(&self->task);
		task_free(&self->task);
		task_init(&self->task);
	}
}

void
io_add(Io* self, Buf* buf)
{
	channel_write(&self->task.channel, buf);
}
