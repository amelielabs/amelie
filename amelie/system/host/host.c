
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
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_json.h>
#include <amelie_config.h>
#include <amelie_user.h>
#include <amelie_auth.h>
#include <amelie_http.h>
#include <amelie_client.h>
#include <amelie_server.h>
#include <amelie_row.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_backup.h>
#include <amelie_repl.h>
#include <amelie_value.h>
#include <amelie_set.h>
#include <amelie_content.h>
#include <amelie_executor.h>
#include <amelie_func.h>
#include <amelie_vm.h>
#include <amelie_compute.h>
#include <amelie_host.h>

hot static void
client_main(void* arg)
{
	auto client = (Client*)arg;
	auto self   = (Host*)client->arg;

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
host_rpc(Rpc* rpc, void* arg)
{
	Host* self = arg;
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
host_main(void* arg)
{
	Host* self = arg;
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
			rpc_execute(rpc, host_rpc, self);
			break;
		}
		}
	}
}

void
host_init(Host*     self,
          HostEvent on_connect,
          void*     on_connect_arg)
{
	self->on_connect     = on_connect;
	self->on_connect_arg = on_connect_arg;
	lock_mgr_init(&self->lock_mgr);
	client_mgr_init(&self->client_mgr);
	auth_init(&self->auth);
	task_init(&self->task);
}

void
host_free(Host* self)
{
	client_mgr_free(&self->client_mgr);
	lock_mgr_free(&self->lock_mgr);
	auth_free(&self->auth);
}

void
host_start(Host* self)
{
	task_create(&self->task, "host", host_main, self);
}

void
host_stop(Host* self)
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
host_add(Host* self, Buf* buf)
{
	channel_write(&self->task.channel, buf);
}
