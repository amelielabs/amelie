
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
		if (opt_int_of(&config()->wal_crc))
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
backend_run(Backend* self, Ctr* ctr, Req* req)
{
	auto dtr = ctr->dtr;
	switch (req->type) {
	case REQ_EXECUTE:
	{
		// create and add transaction to the prepared list (even on error)
		if (ctr->tr == NULL)
		{
			auto tr = tr_create(&self->core.cache);
			tr_begin(tr);
			tr_set_id(tr, dtr->id);
			tr_set_limit(tr, &dtr->limit);
			tr_list_add(&self->core.prepared, tr);
			ctr->tr = tr;
		}

		// execute request
		vm_reset(&self->vm);
		reg_prepare(&self->vm.r, req->code->regs);

		vm_run(&self->vm, dtr->local,
		        ctr->tr,
		        NULL,
		        req->code,
		        req->code_data,
		       &req->arg,
		        req->regs,
		        req->args,
		       &req->result,
		        NULL,
		        req->start);
		break;
	}
	case REQ_REPLAY:
	{
		// create and add transaction to the prepared list (even on error)
		if (ctr->tr == NULL)
		{
			auto tr = tr_create(&self->core.cache);
			tr_begin(tr);
			tr_set_id(tr, dtr->id);
			tr_set_limit(tr, &dtr->limit);
			tr_list_add(&self->core.prepared, tr);
			ctr->tr = tr;
		}

		// replay commands
		backend_replay(self, ctr->tr, &req->arg);
		break;
	}
	case REQ_BUILD:
	{
		auto build = *(Build**)req->arg.start;
		build_execute(build, &self->core);
		break;
	}
	}
}

hot static void
backend_process(Backend* self, Ctr* ctr)
{
	auto queue = &ctr->queue;
	for (;;)
	{
		auto req = req_queue_pop(queue);
		if (! req)
			break;
		if (error_catch(backend_run(self, ctr, req)))
			req->error = msg_error(&am_self()->error);
		req_complete(req);
	}
	ctr_complete(ctr);
}

static void
backend_main(void* arg)
{
	Backend* self = arg;
	auto core = &self->core;
	for (auto active = true; active;)
	{
		CoreEvent core_event;
		switch (core_next(core, &core_event)) {
		case CORE_SHUTDOWN:
			active = false;
			break;
		case CORE_RUN:
			// accept and process incoming core transaction
			backend_process(self, core_event.ctr);
			break;
		case CORE_COMMIT:
			// commit all transaction <= commit id
			tr_commit_list(&core->prepared, &core->cache, core_event.commit);
			break;
		case CORE_ABORT:
			// abort all prepared transactions
			tr_abort_list(&core->prepared, &core->cache);
			break;
		}
	}
}

Backend*
backend_allocate(Db* db, FunctionMgr* function_mgr, int order)
{
	auto self = (Backend*)am_malloc(sizeof(Backend));
	core_init(&self->core, order);
	vm_init(&self->vm, db, &self->core, NULL, NULL, function_mgr);
	task_init(&self->task);
	list_init(&self->link);
	return self;
}

void
backend_free(Backend* self)
{
	vm_free(&self->vm);
	core_free(&self->core);
	am_free(self);
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
		core_shutdown(&self->core);
		task_wait(&self->task);
		task_free(&self->task);
		task_init(&self->task);
	}
}
