
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
#include <amelie_tier>
#include <amelie_storage>
#include <amelie_repl>
#include <amelie_vm>
#include <amelie_frontend.h>
#include <amelie_backend.h>

hot static void
pod_replay(Pod* self, Tr* tr, Buf* arg)
{
	auto pos = arg->start;
	while (pos < arg->position)
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

		assert(uuid_is(&self->part->id.id_table, &cmd->id));

		// replay writes
		auto end = data + cmd->size;
		if (cmd->cmd == CMD_REPLACE)
		{
			while (data < end)
			{
				auto row = row_copy(self->part->heap, (Row*)data);
				part_insert(self->part, tr, true, row);
				data += row_size(row);
			}
		} else {
			while (data < end)
			{
				auto row = (Row*)(data);
				part_delete_by(self->part, tr, row);
				data += row_size(row);
			}
		}
	}
}

hot static void
pod_request(Pod* self, Ltr* ltr, Req* req)
{
	auto dtr = ltr->dtr;
	switch (req->type) {
	case REQ_EXECUTE:
	{
		// create and add transaction to the prepared list (even on error)
		if (ltr->tr == NULL)
		{
			auto track = self->track;
			auto tr = tr_create(&track->cache);
			tr_begin(tr);
			tr_set_id(tr, track->seq++);
			tr_set_limit(tr, &dtr->limit);
			tr_list_add(&track->prepared, tr);
			ltr->tr = tr;
		}

		// execute request
		vm_reset(&self->vm);
		reg_prepare(&self->vm.r, req->code->regs);

		Return ret;
		return_init(&ret);

		vm_run(&self->vm, dtr->local,
		        ltr->tr,
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
		if (ltr->tr == NULL)
		{
			auto track = self->track;
			auto tr = tr_create(&track->cache);
			tr_begin(tr);
			tr_set_id(tr, track->seq++);
			tr_set_limit(tr, &dtr->limit);
			tr_list_add(&track->prepared, tr);
			ltr->tr = tr;
		}

		// replay commands
		pod_replay(self, ltr->tr, &req->arg);
		break;
	}
	case REQ_BUILD:
	{
		auto build = *(Build**)req->arg.start;
		build_execute(build, self->part);
		break;
	}
	}
}

hot static void
pod_run(Pod* self, Ltr* ltr)
{
	// execute incoming requests till close
	auto active = true;
	while (active)
	{
		auto msg = ltr_read(ltr);
		if (msg->id == MSG_LTR_STOP)
		{
			active = false;
			continue;
		}
		auto req = (Req*)msg;
		if (error_catch(pod_request(self, ltr, req)))
		{
			req->error = error_create(&am_self()->error);
			if (! ltr->error)
				ltr->error = req->error;
		}
		active = !req->dispatch->close;
		dispatch_complete(req->dispatch);
	}
	ltr_complete(ltr);
}

hot static void
pod_sync(Pod* self, Ltr* ltr)
{
	auto track         = self->track;
	auto consensus_pod = &track->consensus_pod;
	auto consensus     = &ltr->consensus;

	// commit all transactions <= abort
	auto id = consensus->abort;
	if (unlikely(id > consensus_pod->abort))
	{
		tr_abort_list(&track->prepared, &track->cache, id);
		consensus_pod->abort = id;
	}

	// commit all transactions <= commit
	id = consensus->commit;
	if (id > consensus_pod->commit)
	{
		tr_commit_list(&track->prepared, &track->cache, id);
		consensus_pod->commit = id;
	}
}

static void
pod_main(void* arg)
{
	Pod* self = arg;
	auto track = self->track;
	for (;;)
	{
		auto msg = track_read(track);
		if (msg->id == MSG_STOP)
			break;
		auto ltr = (Ltr*)msg;

		// abort and commit previously prepared transactions
		pod_sync(self, ltr);

		// execute transaction
		pod_run(self, ltr);
	}
}

Pod*
pod_allocate(Part* part)
{
	auto self = (Pod*)am_malloc(sizeof(Pod));
	self->part      =  part;
	self->track     = &part->track;
	self->worker_id = -1;
	vm_init(&self->vm, part, NULL);
	list_init(&self->link);
	return self;
}

void
pod_free(Pod* self)
{
	vm_free(&self->vm);
	am_free(self);
}

void
pod_start(Pod* self, Task* task)
{
	if (self->worker_id != -1)
		return;
	track_set_backend(self->track, task);
	self->worker_id = coroutine_create(pod_main, self);
}

void
pod_stop(Pod* self)
{
	if (self->worker_id == -1)
		return;
	auto worker = coroutine_mgr_find(&am_task->coroutine_mgr, self->worker_id);
	assert(worker);
	Msg stop;
	msg_init(&stop, MSG_STOP);
	track_write(self->track, &stop);
	wait_event(&worker->on_exit, am_self());
	track_set_backend(self->track, NULL);
	self->worker_id = -1;
}
