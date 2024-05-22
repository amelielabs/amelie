
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
#include <sonata_wal.h>
#include <sonata_db.h>
#include <sonata_value.h>
#include <sonata_aggr.h>
#include <sonata_executor.h>
#include <sonata_vm.h>
#include <sonata_shard.h>

hot static void
shard_execute(Shard* self, Trx* trx)
{
	transaction_begin(&trx->trx);

	// execute transaction requests
	for (;;)
	{
		auto req = req_queue_get(&trx->queue);
		if (! req)
			break;

		// execute request
		Exception e;
		if (enter(&e))
		{
			vm_reset(&self->vm);
			vm_run(&self->vm, &trx->trx, trx->code, trx->code_data,
			       &req->arg,
			        trx->cte,
			       &req->result,
			        req->op);
		}

		// respond with OK or ERROR
		Buf* buf;
		if (leave(&e)) {
			buf = msg_error(&so_self()->error);
		} else {
			buf = msg_begin(MSG_OK);
			msg_end(buf);
		}
		channel_write(&trx->src, buf);

		if (e.triggered)
			break;
	}

	// add transaction to the prepared list (even on error)
	trx_list_add(&self->prepared, trx);
}

hot static void
shard_commit(Shard* self, Trx* last)
{
	// commit all prepared transaction till the last one
	trx_list_commit(&self->prepared, last);
}

hot static void
shard_abort(Shard* self, Trx* last)
{
	// abort all prepared transactions
	unused(last);
	trx_list_abort(&self->prepared);
}

static void
shard_recover(Shard* self)
{
	// restore storages related to the current shard
	Exception e;
	if (enter(&e))
	{
		auto db = self->vm.db;
		list_foreach(&db->table_mgr.mgr.list)
		{
			auto table = table_of(list_at(Handle, link));
			table_recover(table, &self->config->id);
		}
	}

	Buf* buf;
	if (leave(&e)) {
		buf = msg_error(&so_self()->error);
	} else {
		buf = msg_begin(MSG_OK);
		msg_end(buf);
	}

	// notify system
	channel_write(global()->control->system, buf);
}

static void
shard_rpc(Rpc* rpc, void* arg)
{
	Shard* self = arg;
	switch (rpc->id) {
	case RPC_STOP:
		unused(self);
		vm_reset(&self->vm);
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
		auto buf = channel_read(&so_task->channel, -1);
		auto msg = msg_of(buf);
		guard(buf_free, buf);

		switch (msg->id) {
		case RPC_BEGIN:
			shard_execute(self, trx_of(buf));
			break;
		case RPC_COMMIT:
			shard_commit(self, trx_of(buf));
			break;
		case RPC_ABORT:
			shard_abort(self, trx_of(buf));
			break;
		case RPC_RECOVER:
			shard_recover(self);
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
	Shard* self = so_malloc(sizeof(*self));
	self->order = 0;
	trx_list_init(&self->prepared);
	task_init(&self->task);
	guard(shard_free, self);
	self->config = shard_config_copy(config);
	vm_init(&self->vm, db, &self->config->id, NULL, NULL, NULL, function_mgr);
	return unguard();
}

void
shard_free(Shard* self)
{
	vm_free(&self->vm);
	if (self->config)
		shard_config_free(self->config);
	so_free(self);
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
