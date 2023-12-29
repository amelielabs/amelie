
//
// indigo
//	
// SQL OLTP database
//

#include <indigo_runtime.h>
#include <indigo_io.h>
#include <indigo_data.h>
#include <indigo_lib.h>
#include <indigo_config.h>
#include <indigo_auth.h>
#include <indigo_client.h>
#include <indigo_server.h>
#include <indigo_def.h>
#include <indigo_transaction.h>
#include <indigo_index.h>
#include <indigo_storage.h>
#include <indigo_wal.h>
#include <indigo_db.h>
#include <indigo_value.h>
#include <indigo_aggr.h>
#include <indigo_request.h>
#include <indigo_vm.h>
#include <indigo_parser.h>
#include <indigo_semantic.h>
#include <indigo_compiler.h>
#include <indigo_shard.h>
#include <indigo_hub.h>

hot static void
client_main(void* arg)
{
	Client* client = arg;
	Hub* self = client->arg;

	// client, backup, replica
	client_mgr_add(&self->client_mgr, client);

	Exception e;
	if (try(&e))
	{
		// authenticate
		client_attach(client);
		client_accept(client, &self->user_cache);

		// main
		switch (client->access) {
		case ACCESS_CLIENT:
			hub_client(self, client);
			break;
		case ACCESS_BACKUP:
			break;
		case ACCESS_REPLICA:
			break;
		default:
			break;
		}
	}

	if (catch(&e))
	{}

	client_mgr_del(&self->client_mgr, client);

	client_detach(client);
	client_free(client);
}

hot static void
native_main(void* arg)
{
	Native* native = arg;
	Hub* self = native->arg;

	// native, relay
	native_set_coroutine(native);
	native_set_coroutine_name(native);

	Exception e;
	if (try(&e))
	{
		// attach channel
		channel_attach(&native->system);

		if (native->relay)
		{
			// create remote connection
			auto client = client_create(ACCESS_CLIENT);
			guard(conn_guard, client_free, client);
			client_set_uri(client, false, &native->uri);
			client_connect(client);

			// notify api
			native_on_connect(native);

			// retransmit data
			relay(client, &native->system, &native->src);
		} else
		{
			// notify api
			native_on_connect(native);

			// main
			hub_native(self, native);
		}
	}

	if (catch(&e))
	{}

	// detach ipc
	channel_detach(&native->system);
	native_on_disconnect(native);
}

static void
hub_rpc(Rpc* rpc, void* arg)
{
	Hub* self = arg;
	switch (rpc->id) {
	case RPC_USER_CACHE_SYNC:
	{
		UserCache* with = rpc_arg_ptr(rpc, 0);
		user_cache_sync(&self->user_cache, with);
		break;
	}
	case RPC_SESSION_LOCK:
	{
		// take exclusive session lock
		assert(! self->session_locker);
		self->session_locker = lock_lock(&self->session_lock, false);
		break;
	}
	case RPC_SESSION_UNLOCK:
	{
		assert(self->session_locker);
		lock_unlock(self->session_locker);
		self->session_locker = NULL;
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
hub_main(void* arg)
{
	Hub* self = arg;
	bool stop = false;
	while (! stop)
	{
		auto buf = channel_read(&mn_task->channel, -1);
		auto msg = msg_of(buf);
		guard(buf_guard, buf_free, buf);

		switch (msg->id) {
		case MSG_CLIENT:
		{
			// remote client
			Client* client = *(void**)msg->data;
			client->arg = self;
			coroutine_create(client_main, client);
			break;
		}
		case MSG_NATIVE:
		{
			// native client
			Native* native = *(void**)msg->data;
			native->arg = self;
			coroutine_create(native_main, native);
			break;
		}
		default:
		{
			// command
			stop = msg->id == RPC_STOP;
			rpc_execute(buf, hub_rpc, self);
			break;
		}
		}
	}
}

void
hub_init(Hub* self, Share* share, HubIf* iface)
{
	self->iface              = iface;
	self->share              = *share;
	self->share.req_cache    = &self->req_cache;
	self->share.session_lock = &self->session_lock;
	self->session_locker     = NULL;

	locker_cache_init(&self->locker_cache);
	lock_init(&self->session_lock, &self->locker_cache);
	req_cache_init(&self->req_cache);
	client_mgr_init(&self->client_mgr);
	user_cache_init(&self->user_cache);
	task_init(&self->task);
}

void
hub_free(Hub* self)
{
	locker_cache_free(&self->locker_cache);
	req_cache_free(&self->req_cache);
	client_mgr_free(&self->client_mgr);
	user_cache_free(&self->user_cache);
}

void
hub_start(Hub* self)
{
	task_create(&self->task, "hub", hub_main, self);
}

void
hub_stop(Hub* self)
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
hub_add(Hub* self, Buf* buf)
{
	channel_write(&self->task.channel, buf);
}
