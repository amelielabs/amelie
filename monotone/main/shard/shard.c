
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
#include <monotone_def.h>
#include <monotone_transaction.h>
#include <monotone_storage.h>
#include <monotone_wal.h>
#include <monotone_db.h>
#include <monotone_value.h>
#include <monotone_aggr.h>
#include <monotone_request.h>
#include <monotone_vm.h>
#include <monotone_parser.h>
#include <monotone_compiler.h>
#include <monotone_shard.h>

hot static void
shard_execute(Shard* self, Req* req)
{
	transaction_begin(&req->trx);

	Portal portal;
	portal_init(&portal, portal_to_channel, &req->src);

	// execute
	Exception e;
	if (try(&e))
	{
		vm_reset(&self->vm);
		vm_run(&self->vm, &req->trx, NULL, req->cmd,
		       &req->code,
		        req->code_data, &portal);
	}

	bool abort = false;
	Buf* reply;
	if (catch(&e))
	{
		// error
		reply = make_error(&mn_self()->error);
		portal_write(&portal, reply);
		abort = true;
	}

	if (likely(! abort))
	{
		// add transaction to the prepared list
		req_list_add(&self->prepared, req);

		// wait for previous transaction to be commited before
		// acknowledge
		if (self->prepared.list_count > 1)
			return;
	} else
	{
		transaction_abort(&req->trx);
	}

	// OK
	req->ok = true;
	reply = msg_create(MSG_OK);
	msg_end(reply);
	portal_write(&portal, reply);
}

hot static void
shard_commit(Shard* self, Req* req)
{
	// commit transaction
	transaction_commit(&req->trx);

	// acknowledge all pending prepared transaction
	list_foreach_after(&self->prepared.list, &req->link)
	{
		auto ref = list_at(Req, link);
		if (ref->ok)
			continue;

		// OK
		auto reply = msg_create(MSG_OK);
		msg_end(reply);
		channel_write(&ref->src, reply);
		ref->ok = true;
	}

	// done
	req_list_remove(&self->prepared, req);

	// put request back to the cache
	req_cache_push(req->cache, req);
}

hot static void
shard_abort(Shard* self, Req* req)
{
	// abort all prepared transactions up to current one
	// in reverse order
	list_foreach_reverse_safe(&self->prepared.list)
	{
		auto ref = list_at(Req, link);

		// abort
		transaction_abort(&ref->trx);

		req_list_remove(&self->prepared, ref);
		if (ref == req)
			break;

		// send error to the pending transaction
		Str msg;
		str_init(&msg);
		str_set_cstr(&msg, "transaction conflict, abort");
		auto reply = make_error_as(ERROR_CONFLICT, &msg);
		channel_write(&ref->src, reply);

		// OK
		reply = msg_create(MSG_OK);
		msg_end(reply);
		channel_write(&ref->src, reply);
		ref->ok = true;
	}

	// put request back to the cache
	req_cache_push(req->cache, req);
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

		switch (msg->id) {
		case RPC_REQUEST:
			shard_execute(self, req_of(buf));
			break;
		case RPC_REQUEST_COMMIT:
			shard_commit(self, req_of(buf));
			break;
		case RPC_REQUEST_ABORT:
			shard_abort(self, req_of(buf));
			break;
		default:
		{
			stop = msg->id == RPC_STOP;
			rpc_execute(buf, shard_rpc, self);
			break;
		}
		}
	}
}

Shard*
shard_allocate(ShardConfig* config, Db* db, FunctionMgr* function_mgr)
{
	Shard* self = mn_malloc(sizeof(*self));
	self->order = 0;
	req_list_init(&self->prepared);
	task_init(&self->task);
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
		task_init(&self->task);
	}
}
