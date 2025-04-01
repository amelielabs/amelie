
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

hot static void
backend_replay(Backend* self, Tr* tr, Buf* arg)
{
	auto db = self->vm.db;
	for (auto pos = arg->start; pos < arg->position;)
	{
		// command
		auto cmd = *(RecordCmd**)pos;
		pos += sizeof(uint8_t**);

		// data
		auto data = *(uint8_t**)pos;
		pos += sizeof(uint8_t**);

		// validate command crc
		if (var_int_of(&config()->wal_crc))
			if (unlikely(! record_validate_cmd(cmd, data)))
				error("replay: record command crc mismatch");

		// partition
		auto part = part_mgr_find(&db->part_mgr, cmd->partition);
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

hot static void
backend_process(Backend* self, Req* req)
{
	auto route = &self->route;
	switch (req->arg.type) {
	case REQ_EXECUTE:
	{
		auto arg = &req->arg;
		auto dtr = req->arg.dtr;

		// create and add transaction to the prepared list (even on error)
		auto tr = tr_create(&route->cache);
		tr_begin(tr);
		tr_set_id(tr, dtr->id);
		tr_set_limit(tr, &dtr->limit);
		tr_list_add(&route->prepared, tr);
		arg->tr = tr;

		// execute request
		vm_reset(&self->vm);
		vm_run(&self->vm, dtr->local,
		        route,
		        dtr,
		        tr,
		        dtr->program->code_backend,
		        dtr->program->code_data,
		       &arg->arg,
		       &dtr->cte,
		       &arg->result,
		        NULL,
		        arg->start);
		break;
	}
	case REQ_REPLAY:
	{
		auto arg = &req->arg;
		auto dtr = req->arg.dtr;

		// create and add transaction to the prepared list (even on error)
		auto tr = tr_create(&route->cache);
		tr_begin(tr);
		tr_set_id(tr, dtr->id);
		tr_list_add(&route->prepared, tr);
		arg->tr = tr;

		// replay commands
		backend_replay(self, tr, &arg->arg);
		break;
	}
	case REQ_COMMIT:
	{
		// commit all prepared transaction till the last one by id
		auto id = *buf_u64(&req->arg.arg);
		tr_commit_list(&route->prepared, &route->cache, id);
		break;
	}
	case REQ_ABORT:
		// abort all prepared transactions
		tr_abort_list(&route->prepared, &route->cache);
		break;
	case REQ_BUILD:
		//auto build = *(Build**)msg->data;
		//build_execute(build, &self->worker->id);
		break;
	}
}

static void
backend_main(void* arg)
{
	Backend* self = arg;
	auto route = &self->route;
	for (;;)
	{
		auto req = route_get(route);
		if (! req)
			break;
		if (error_catch(backend_process(self, req)))
			req->arg.error = msg_error(&am_self()->error);
		req_complete(req);
	}
}

void
backend_init(Backend*     self,
             Db*          db,
             Executor*    executor,
             FunctionMgr* function_mgr)
{
	route_init(&self->route);
	vm_init(&self->vm, db, executor, function_mgr);
	task_init(&self->task);
	list_init(&self->link);
}

void
backend_free(Backend* self)
{
	vm_free(&self->vm);
	route_free(&self->route);
}

void
backend_start(Backend* self)
{
	task_create(&self->task, "backend", backend_main, self);
}

void
backend_stop(Backend* self)
{
	if (task_active(&self->task))
	{
		route_shutdown(&self->route);
		task_wait(&self->task);
		task_free(&self->task);
		task_init(&self->task);
	}
}
