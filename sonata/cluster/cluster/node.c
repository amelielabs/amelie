
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
node_execute(Node* self, Trx* trx)
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
node_commit(Node* self, Trx* last)
{
	// commit all prepared transaction till the last one
	trx_list_commit(&self->prepared, last);
}

hot static void
node_abort(Node* self, Trx* last)
{
	// abort all prepared transactions
	unused(last);
	trx_list_abort(&self->prepared);
}

static void
node_recover(Node* self)
{
	// restore partitions related to the current node
	Exception e;
	if (enter(&e)) {
		recover(self->vm.db, &self->config->id);
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
node_rpc(Rpc* rpc, void* arg)
{
	Node* self = arg;
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
node_main(void* arg)
{
	Node* self = arg;

	bool stop = false;
	while (! stop)
	{
		auto buf = channel_read(&so_task->channel, -1);
		auto msg = msg_of(buf);
		guard(buf_free, buf);

		switch (msg->id) {
		case RPC_BEGIN:
			node_execute(self, trx_of(buf));
			break;
		case RPC_COMMIT:
			node_commit(self, trx_of(buf));
			break;
		case RPC_ABORT:
			node_abort(self, trx_of(buf));
			break;
		case RPC_RECOVER:
			node_recover(self);
			break;
		default:
		{
			stop = msg->id == RPC_STOP;
			rpc_execute(buf, node_rpc, self);
			break;
		}
		}
	}
}

Node*
node_allocate(NodeConfig* config, Db* db, FunctionMgr* function_mgr)
{
	auto self = (Node*)so_malloc(sizeof(Node));
	self->config = NULL;
	route_init(&self->route, &self->task.channel);
	trx_list_init(&self->prepared);
	list_init(&self->link);
	vm_init(&self->vm, db, NULL, NULL, NULL, NULL, function_mgr);
	task_init(&self->task);
	guard(node_free, self);
	self->config = node_config_copy(config);
	self->vm.node = &self->config->id;
	return unguard();
}

void
node_free(Node* self)
{
	vm_free(&self->vm);
	if (self->config)
		node_config_free(self->config);
	so_free(self);
}

void
node_start(Node* self)
{
	task_create(&self->task, "node", node_main, self);
}

void
node_stop(Node* self)
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
