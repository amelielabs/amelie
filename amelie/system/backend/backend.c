
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
#include <amelie_json.h>
#include <amelie_config.h>
#include <amelie_user.h>
#include <amelie_auth.h>
#include <amelie_http.h>
#include <amelie_client.h>
#include <amelie_server.h>
#include <amelie_row.h>
#include <amelie_heap.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_backup.h>
#include <amelie_repl.h>
#include <amelie_value.h>
#include <amelie_set.h>
#include <amelie_content.h>
#include <amelie_executor.h>
#include <amelie_func.h>
#include <amelie_vm.h>
#include <amelie_parser.h>
#include <amelie_compiler.h>
#include <amelie_frontend.h>
#include <amelie_backend.h>

static void
backend_replay(Backend* self, Tr* tr, Req* req)
{
	auto db = self->vm.db;
	for (auto pos = req->arg.start; pos < req->arg.position;)
	{
		// command
		auto cmd = *(RecordCmd**)pos;
		pos += sizeof(uint8_t**);

		// data
		auto data = *(uint8_t**)pos;
		pos += sizeof(uint8_t**);

		// validate command crc
		if (opt_int_of(&config()->wal_crc))
			if (unlikely(! record_validate_cmd(cmd, data)))
				error("replay: record command crc mismatch");

		// partition
		auto part = table_mgr_find_partition(&db->table_mgr, cmd->partition);
		if (! part)
			error("failed to find partition %" PRIu64, cmd->partition);

		// replay writes
		auto end = data + cmd->size;
		if (cmd->cmd == CMD_REPLACE)
		{
			while (data < end)
			{
				auto row = row_copy(&part->heap, (Row*)data);
				part_insert(part, tr, true, row);
				data += row_size(row);
			}
		} else {
			while (data < end)
			{
				auto row = (Row*)(data);
				part_delete_by(part, tr, row);
				data += row_size(row);
			}
		}
	}
}

hot static inline void
backend_execute(Backend* self, Tr* tr, Req* req)
{
	tr_set_limit(tr, req->limit);
	vm_reset(&self->vm);
	vm_run(&self->vm, req->local,
	        tr,
	        req->program->code_backend,
	        req->program->code_data,
	       &req->arg,
	        req->regs,
	        req->args,
	       &req->result,
	        NULL,
	        req->start);
}

hot static inline void
backend_req(Backend* self, Tr* tr, Req* req)
{
	switch (req->type) {
	case REQ_EXECUTE:
		backend_execute(self, tr, req);
		break;
	case REQ_REPLAY:
		backend_replay(self, tr, req);
		break;
	case REQ_SHUTDOWN:
		break;
	default:
		break;
	}
}

hot static void
backend_process(Backend* self, Pipe* pipe)
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
		auto on_error = error_catch( backend_req(self, tr, req) );

		// MSG_READY on pipe completion
		// MSG_ERROR on pipe completion with error
		// MSG_OK on execution
		auto is_last = req->shutdown;
		Buf* buf;
		if (on_error) {
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

		if (is_last || on_error)
			break;
	}

	// add transaction to the prepared list (even on error)
	tr_list_add(&self->prepared, tr);
}

static void
backend_rpc(Rpc* rpc, void* arg)
{
	Backend* self = arg;
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
backend_main(void* arg)
{
	Backend* self = arg;
	for (;;)
	{
		auto buf = channel_read(&am_task->channel, -1);
		auto msg = msg_of(buf);
		defer_buf(buf);

		switch (msg->id) {
		case RPC_BEGIN:
			backend_process(self, pipe_of(buf));
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
			build_execute(build, &self->worker->id);
			break;
		}
		default:
		{
			auto rpc = rpc_of(buf);
			rpc_execute(rpc, backend_rpc, self);
			break;
		}
		}

		if (msg->id == RPC_STOP)
			break;
	}
}

Backend*
backend_allocate(Worker* worker, Db* db, FunctionMgr* function_mgr)
{
	auto self = (Backend*)am_malloc(sizeof(Backend));
	self->worker = worker;
	tr_list_init(&self->prepared);
	tr_cache_init(&self->cache);
	list_init(&self->link);
	vm_init(&self->vm, db, NULL, NULL, NULL, function_mgr);
	self->vm.backend = &worker->id;
	task_init(&self->task);
	return self;
}

void
backend_free(Backend* self)
{
	vm_free(&self->vm);
	tr_cache_free(&self->cache);
	am_free(self);
}

void
backend_start(Backend* self)
{
	Uuid id;
	uuid_set(&id, &self->worker->config->id);
	char name[9];
	uuid_get_short(&id, name, sizeof(name));

	task_create(&self->task, name, backend_main, self);
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

void
backend_sync(Backend* self)
{
	rpc(&self->task.channel, RPC_SYNC, 0);
}
