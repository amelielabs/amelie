
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
#include <amelie_data.h>
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
#include <amelie_value.h>
#include <amelie_store.h>
#include <amelie_executor.h>
#include <amelie_vm.h>
#include <amelie_parser.h>
#include <amelie_planner.h>
#include <amelie_compiler.h>
#include <amelie_backup.h>
#include <amelie_repl.h>
#include <amelie_cluster.h>

static void
compute_replay(Compute* self, Tr* tr, Req* req)
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

		// replay write
		if (type == LOG_REPLACE)
			part_insert(part, tr, true, &data);
		else
			part_delete_by(part, tr, &data);
	}
}

hot static inline void
compute_load(Compute* self, Tr* tr, Req* req)
{
	// execute batch INSERT
	for (auto pos = req->arg.start; pos < req->arg.position;)
	{
		// row offset
		int64_t offset;
		data_read_integer(&pos, &offset);
		auto data = req->arg_buf->start + offset;

		auto part = part_list_match(&req->arg_table->part_list, &self->node->id);
		part_insert(part, tr, false, &data);
	}
}

hot static inline void
compute_execute(Compute* self, Tr* tr, Req* req)
{
	tr_set_limit(tr, req->limit);
	vm_reset(&self->vm);
	vm_run(&self->vm, req->local,
	        tr,
	        req->program->code_node,
	        req->program->code_data,
	       &req->arg,
	        req->arg_buf,
	        req->args,
	        req->cte,
	       &req->result,
	        req->start);
}

hot static void
compute_process(Compute* self, Pipe* pipe)
{
	auto tr = tr_create(&self->cache);
	tr_begin(tr);

	// read pipe and execute requests
	pipe->tr = tr;
	for (;;)
	{
		auto req = req_queue_pop(&pipe->queue);
		assert(req);

		// execute request
		Exception e;
		if (enter(&e))
		{
			switch (req->type) {
			case REQ_EXECUTE:
				compute_execute(self, tr, req);
				break;
			case REQ_LOAD:
				compute_load(self, tr, req);
				break;
			case REQ_REPLAY:
				compute_replay(self, tr, req);
				break;
			case REQ_SHUTDOWN:
				// force shutdown
				break;
			default:
				break;
			}
		}

		// MSG_READY on pipe completion
		// MSG_ERROR on pipe completion with error
		// MSG_OK on execution
		auto is_last = req->shutdown;
		Buf* buf;
		if (leave(&e)) {
			buf = msg_error(&am_self()->error);
		} else
		{
			int id;
			if (is_last)
				id = MSG_READY;
			else
				id = MSG_OK;
			buf = msg_create(id);
			msg_end(buf);
		}
		channel_write(&pipe->src, buf);

		if (is_last || e.triggered)
			break;
	}

	// add transaction to the prepared list (even on error)
	tr_list_add(&self->prepared, tr);
}

static void
compute_rpc(Rpc* rpc, void* arg)
{
	Compute* self = arg;
	switch (rpc->id) {
	case RPC_SYNC:
		// do nothing, just respond
		break;
	case RPC_STOP:
		unused(self);
		vm_reset(&self->vm);
		break;
	default:
		break;
	}
}

static void
compute_main(void* arg)
{
	Compute* self = arg;
	for (;;)
	{
		auto buf = channel_read(&am_task->channel, -1);
		auto msg = msg_of(buf);
		guard_buf(buf);

		switch (msg->id) {
		case RPC_BEGIN:
			compute_process(self, pipe_of(buf));
			break;
		case RPC_COMMIT:
		{
			// commit all prepared transaction till the last one
			tr_commit_list(&self->prepared, &self->cache, tr_of(buf));
			break;
		}
		case RPC_ABORT:
		{
			// abort all prepared transactions
			tr_abort_list(&self->prepared, &self->cache);
			break;
		}
		case RPC_BUILD:
		{
			auto build = *(Build**)msg->data;
			build_execute(build, &self->node->id);
			break;
		}
		default:
		{
			auto rpc = rpc_of(buf);
			rpc_execute(rpc, compute_rpc, self);
			break;
		}
		}

		if (msg->id == RPC_STOP)
			break;
	}
}

Compute*
compute_allocate(Node* node, Db* db, FunctionMgr* function_mgr)
{
	auto self = (Compute*)am_malloc(sizeof(Compute));
	self->node = node;
	tr_list_init(&self->prepared);
	tr_cache_init(&self->cache);
	list_init(&self->link);
	vm_init(&self->vm, db, NULL, NULL, NULL, NULL, function_mgr);
	self->vm.node = &node->id;
	task_init(&self->task);
	return self;
}

void
compute_free(Compute* self)
{
	vm_free(&self->vm);
	tr_cache_free(&self->cache);
	am_free(self);
}

void
compute_start(Compute* self)
{
	task_create(&self->task, "node", compute_main, self);
}

void
compute_stop(Compute* self)
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
compute_sync(Compute* self)
{
	rpc(&self->task.channel, RPC_SYNC, 0);
}
