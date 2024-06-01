
//
// sonata.
//
// Real-Time SQL Database.
//

#include <sonata_runtime.h>
#include <sonata_io.h>
#include <sonata_lib.h>
#include <sonata_data.h>
#include <sonata_config.h>
#include <sonata_user.h>
#include <sonata_node.h>
#include <sonata_http.h>
#include <sonata_client.h>
#include <sonata_server.h>
#include <sonata_def.h>
#include <sonata_transaction.h>
#include <sonata_index.h>
#include <sonata_partition.h>
#include <sonata_wal.h>
#include <sonata_db.h>
#include <sonata_value.h>
#include <sonata_aggr.h>
#include <sonata_executor.h>
#include <sonata_vm.h>
#include <sonata_parser.h>
#include <sonata_semantic.h>
#include <sonata_compiler.h>
#include <sonata_backup.h>
#include <sonata_repl.h>
#include <sonata_cluster.h>

hot static void
backend_execute(Backend* self, Trx* trx)
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
backend_commit(Backend* self, Trx* last)
{
	// commit all prepared transaction till the last one
	trx_list_commit(&self->prepared, last);
}

hot static void
backend_abort(Backend* self, Trx* last)
{
	// abort all prepared transactions
	unused(last);
	trx_list_abort(&self->prepared);
}

static void
backend_recover(Backend* self)
{
	// restore partitions related to the current backend
	Exception e;
	if (enter(&e)) {
		recover(self->vm.db, &self->node->config->id);
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
backend_rpc(Rpc* rpc, void* arg)
{
	Backend* self = arg;
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
backend_main(void* arg)
{
	Backend* self = arg;

	bool stop = false;
	while (! stop)
	{
		auto buf = channel_read(&so_task->channel, -1);
		auto msg = msg_of(buf);
		guard(buf_free, buf);

		switch (msg->id) {
		case RPC_BEGIN:
			backend_execute(self, trx_of(buf));
			break;
		case RPC_COMMIT:
			backend_commit(self, trx_of(buf));
			break;
		case RPC_ABORT:
			backend_abort(self, trx_of(buf));
			break;
		case RPC_RECOVER:
			backend_recover(self);
			break;
		default:
		{
			stop = msg->id == RPC_STOP;
			rpc_execute(buf, backend_rpc, self);
			break;
		}
		}
	}
}

Backend*
backend_allocate(Node* node, Db* db, FunctionMgr* function_mgr)
{
	auto self = (Backend*)so_malloc(sizeof(Backend));
	self->order = 0;
	self->node  = node;
	trx_list_init(&self->prepared);
	task_init(&self->task);
	vm_init(&self->vm, db, &node->config->id, NULL, NULL, NULL, function_mgr);
	list_init(&self->link);
	return self;
}

void
backend_free(Backend* self)
{
	vm_free(&self->vm);
	so_free(self);
}

void
backend_start(Backend* self)
{
	task_create(&self->task, "backend", backend_main, self);
}

void
backend_stop(Backend* self)
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
