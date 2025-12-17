
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

#include <amelie_runtime>
#include <amelie_server>
#include <amelie_storage>
#include <amelie_repl>
#include <amelie_vm>
#include <amelie_frontend.h>
#include <amelie_backend.h>

hot static void
backend_replay(Tr* tr, Buf* arg)
{
	auto storage = share()->storage;
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
		auto part = part_mgr_find(&storage->part_mgr, cmd->partition);
		if (! part)
			error("failed to find partition %" PRIu32, cmd->partition);

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
backend_execute(Backend* self, Ctr* ctr, Req* req)
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
			tr_set_tsn(tr, dtr->tsn);
			tr_set_limit(tr, &dtr->limit);
			tr_list_add(&self->core.prepared, tr);
			ctr->tr = tr;
		}

		// execute request
		vm_reset(&self->vm);
		reg_prepare(&self->vm.r, req->code->regs);

		Return ret;
		return_init(&ret);

		vm_run(&self->vm, dtr->local,
		        ctr->tr,
		        NULL,
		        req->code,
		        req->code_data,
		       &req->arg,
		       (Value*)req->refs.start,
		        NULL,
		       &ret,
		        false,
		        req->start);

		if (ret.value)
			value_move(&req->result, ret.value);
		break;
	}
	case REQ_REPLAY:
	{
		// create and add transaction to the prepared list (even on error)
		if (ctr->tr == NULL)
		{
			auto tr = tr_create(&self->core.cache);
			tr_begin(tr);
			tr_set_tsn(tr, dtr->tsn);
			tr_set_limit(tr, &dtr->limit);
			tr_list_add(&self->core.prepared, tr);
			ctr->tr = tr;
		}

		// replay commands
		backend_replay(ctr->tr, &req->arg);
		break;
	}
	case REQ_BUILD:
	{
		auto build = *(Build**)req->arg.start;
		build_execute(build, &self->core);
		break;
	}
	case REQ_SYNC:
	{
		// do nothing
		break;
	}
	}
}

hot static void
backend_commit(Backend* self, Req* req)
{
	auto core           = &self->core;
	auto core_consensus = &core->consensus;
	auto consensus      = &req->ctr->dtr->consensus;

	// abort all transactions tsn_max of which >= abort
	auto id = consensus->abort;
	if (unlikely(id > core_consensus->abort))
	{
		tr_abort_list(&core->prepared, &core->cache, id);
		core_consensus->abort = id;
	}

	// commit all transactions <= commit
	id = consensus->commit;
	if (id > core_consensus->commit)
	{
		tr_commit_list(&core->prepared, &core->cache, id);
		core_consensus->commit = id;
	}
}

hot static void
backend_main(void* arg)
{
	Backend* self = arg;
	for (;;)
	{
		auto msg = task_recv();
		if (msg->id == MSG_STOP)
			break;

		// process incoming request
		assert(msg->id == MSG_REQ);
		auto req = (Req*)msg;

		// abort and commit previously prepared transactions
		backend_commit(self, req);

		// execute request
		auto ctr = req->ctr;
		if (error_catch(backend_execute(self, ctr, req)))
		{
			req->error = error_create(&am_self()->error);
			if (! ctr->error)
				ctr->error = req->error;
		}

		// group completion
		dispatch_complete(req->dispatch);
	}
}

Backend*
backend_allocate(int order)
{
	auto self = (Backend*)am_malloc(sizeof(Backend));
	core_init(&self->core, &self->task, order);
	vm_init(&self->vm, &self->core, NULL);
	task_init(&self->task);
	list_init(&self->link);
	return self;
}

void
backend_free(Backend* self)
{
	vm_free(&self->vm);
	core_free(&self->core);
	task_free(&self->task);
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
	}
}
