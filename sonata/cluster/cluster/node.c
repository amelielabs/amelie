
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
#include <sonata_row.h>
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
node_execute_write(Node* self, Trx* trx, Req* req)
{
	// execute DML operations
	auto db = self->vm.db;
	for (auto pos = req->arg.start; pos < req->arg.position;)
	{
		// [meta offset, data offset]

		// meta_offset
		int64_t meta_offset;
		data_read_integer(&pos, &meta_offset);

		// data_offset
		int64_t data_offset;
		data_read_integer(&pos, &data_offset);

		uint8_t* meta = req->arg_start + meta_offset;
		uint8_t* data = req->arg_start + data_offset;

		// type
		int64_t type;
		data_read_integer(&meta, &type);

		// partition
		int64_t partition_id;
		data_read_integer(&meta, &partition_id);
		auto part = table_mgr_find_partition(&db->table_mgr, partition_id);
		if (! part)
			error("failed to find partition %" PRIu64, partition_id);

		// todo: serial recover

		// replay write
		if (type == LOG_REPLACE)
			part_insert(part, &trx->trx, true, &data);
		else
			part_delete_by(part, &trx->trx, &data);
	}
}

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
			if (req->arg_start)
			{
				node_execute_write(self, trx, req);
			} else
			{
				vm_reset(&self->vm);
				vm_run(&self->vm, &trx->trx, trx->code, trx->code_data,
				       &req->arg,
				        trx->cte,
				       &req->result,
				        req->op);
			}
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
	// abort all prepared transactions starting from the last one
	trx_list_abort(&self->prepared, last);
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
	for (;;)
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
		case RPC_BUILD:
		{
			auto build = *(Build**)msg->data;
			build_execute(build, &self->config->id);
			break;
		}
		default:
			rpc_execute(buf, node_rpc, self);
			break;
		}

		if (msg->id == RPC_STOP)
			break;
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
