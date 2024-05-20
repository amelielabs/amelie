
//
// sonata.
//
// SQL Database for JSON.
//

#include <sonata_runtime.h>
#include <sonata_io.h>
#include <sonata_lib.h>
#include <sonata_data.h>
#include <sonata_config.h>
#include <sonata_auth.h>
#include <sonata_http.h>
#include <sonata_client.h>
#include <sonata_server.h>
#include <sonata_def.h>
#include <sonata_transaction.h>
#include <sonata_index.h>
#include <sonata_storage.h>
#include <sonata_db.h>
#include <sonata_value.h>
#include <sonata_aggr.h>
#include <sonata_executor.h>
#include <sonata_vm.h>
#include <sonata_frontend.h>

hot static void
client_main(void* arg)
{
	auto client = (Client*)arg;
	auto self   = (Frontend*)client->arg;

	client_mgr_add(&self->client_mgr, client);

	Exception e;
	if (enter(&e))
	{
		client_attach(client);
		client_accept(client);

		// process client connection
		self->on_connect(self, client);
	}

	if (leave(&e))
	{ }

	client_mgr_del(&self->client_mgr, client);

	client_close(client);
	client_free(client);
}

static void
frontend_rpc(Rpc* rpc, void* arg)
{
	Frontend* self = arg;
	switch (rpc->id) {
	case RPC_SYNC:
	{
		// sync user caches
		UserCache* with = rpc_arg_ptr(rpc, 0);
		user_cache_sync(&self->user_cache, with);
		break;
	}
	case RPC_LOCK:
	{
		// take exclusive session lock
		assert(! self->locker);
		self->locker = lock_lock(&self->lock, false);
		break;
	}
	case RPC_UNLOCK:
	{
		assert(self->locker);
		lock_unlock(self->locker);
		self->locker = NULL;
		break;
	}
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
frontend_main(void* arg)
{
	Frontend* self = arg;
	bool stop = false;
	while (! stop)
	{
		auto buf = channel_read(&so_task->channel, -1);
		auto msg = msg_of(buf);
		guard(buf_free, buf);

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
			rpc_execute(buf, frontend_rpc, self);
			break;
		}
		}
	}
}

void
frontend_init(Frontend*     self,
              FrontendEvent on_connect,
              void*         on_connect_arg)
{
	self->on_connect     = on_connect;
	self->on_connect_arg = on_connect_arg;
	self->locker         = NULL;
	locker_cache_init(&self->locker_cache);
	lock_init(&self->lock, &self->locker_cache);
	client_mgr_init(&self->client_mgr);
	req_cache_init(&self->req_cache);
	trx_cache_init(&self->trx_cache);
	user_cache_init(&self->user_cache);
	task_init(&self->task);
}

void
frontend_free(Frontend* self)
{
	locker_cache_free(&self->locker_cache);
	client_mgr_free(&self->client_mgr);
	trx_cache_free(&self->trx_cache);
	req_cache_free(&self->req_cache);
	user_cache_free(&self->user_cache);
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
		rpc(&self->task.channel, RPC_STOP, 0);
		task_wait(&self->task);
		task_free(&self->task);
		task_init(&self->task);
	}
}

void
frontend_add(Frontend* self, Buf* buf)
{
	channel_write(&self->task.channel, buf);
}
