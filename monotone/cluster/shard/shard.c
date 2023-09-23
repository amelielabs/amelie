
//
// monotone
//	
// SQL OLTP database
//

#include <monotone_runtime.h>
#include <monotone_io.h>
#include <monotone_data.h>
#include <monotone_lib.h>
#include <monotone_config.h>
#include <monotone_auth.h>
#include <monotone_client.h>
#include <monotone_server.h>
#include <monotone_shard.h>

static void
shard_request(Shard* self, Request* req)
{
	unused(self);

	auto ro = req->ro;
	auto on_commit = condition_create();
	req->on_commit = on_commit;

	// OK
	auto reply = msg_create(MSG_OK);
	encode_integer(reply, false);
	msg_end(reply);
	channel_write(&req->src, reply);

	// wait for commit
	if (! ro)
		condition_wait(on_commit, -1);
	condition_free(on_commit);
}

static void
shard_rpc(Rpc* rpc, void* arg)
{
	Shard* self = arg;
	unused(self);
	switch (rpc->id) {
	case RPC_STOP:
		break;
	default:
		break;
	}
}

static void
shard_main(void* arg)
{
	Shard* self = arg;
	bool stop = false;
	while (! stop)
	{
		auto buf = channel_read(&mn_task->channel, -1);
		auto msg = msg_of(buf);
		guard(buf_guard, buf_free, buf);

		if (msg->id == RPC_REQUEST)
		{
			// request
			auto req = request_of(buf);
			shard_request(self, req);
		} else
		{
			// rpc
			stop = msg->id == RPC_STOP;
			rpc_execute(buf, shard_rpc, self);
		}
	}
}

Shard*
shard_allocate(ShardConfig* config)
{
	Shard* self;
	self = mn_malloc(sizeof(*self));
	self->config = shard_config_copy(config);
	guard(self_guard, shard_free, self);
	task_init(&self->task, mn_task->buf_cache);
	return unguard(&self_guard);
}

void
shard_free(Shard* self)
{
	if (self->config)
		shard_config_free(self->config);
	mn_free(self);
}

void
shard_start(Shard* self)
{
	task_create(&self->task, "shard", shard_main, self);
}

void
shard_stop(Shard* self)
{
	// send stop request
	if (task_active(&self->task))
	{
		rpc(&self->task.channel, RPC_STOP, 0);
		task_wait(&self->task);
		task_free(&self->task);
		task_init(&self->task, mn_task->buf_cache);
	}
}
