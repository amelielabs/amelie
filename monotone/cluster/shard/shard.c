
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
#include <monotone_schema.h>
#include <monotone_transaction.h>
#include <monotone_storage.h>
#include <monotone_wal.h>
#include <monotone_db.h>
#include <monotone_value.h>
#include <monotone_aggr.h>
#include <monotone_vm.h>
#include <monotone_parser.h>
#include <monotone_compiler.h>
#include <monotone_shard.h>

hot static void
shard_request(Shard* self, Request* req)
{
	unused(self);

	auto ro = req->ro;
	auto on_commit = condition_create();
	req->on_commit = on_commit;

	transaction_begin(&req->trx);

	Portal portal;
	portal_init(&portal, portal_to_channel, &req->src);

	// execute
	Exception e;
	if (try(&e))
	{
		vm_reset(&self->vm);
		vm_run(&self->vm, &req->trx, &req->code, 0, NULL, req->cmd, &portal);
	}

	Buf* reply;
	if (catch(&e))
	{
		// error
		reply = make_error(&mn_self()->error);
		portal_write(&portal, reply);
	}

	// OK
	reply = msg_create(MSG_OK);
	encode_integer(reply, false);
	msg_end(reply);
	portal_write(&portal, reply);

	// wait for commit
	bool abort = false;
	if (! ro)
	{
		condition_wait(on_commit, -1);
		abort = req->abort;
	}

	if (unlikely(abort))
		transaction_abort(&req->trx);
	else
		transaction_commit(&req->trx);
	condition_free(on_commit);
}

static void
shard_rpc(Rpc* rpc, void* arg)
{
	Shard* self = arg;
	switch (rpc->id) {
	case RPC_STOP:
		unused(self);
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
shard_allocate(ShardConfig* config, Db* db, FunctionMgr* function_mgr)
{
	Shard* self = mn_malloc(sizeof(*self));
	self->order = 0;
	task_init(&self->task, mn_task->buf_cache);
	guard(self_guard, shard_free, self);
	self->config = shard_config_copy(config);
	vm_init(&self->vm, db, function_mgr, &self->config->id);
	return unguard(&self_guard);
}

void
shard_free(Shard* self)
{
	vm_free(&self->vm);
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
