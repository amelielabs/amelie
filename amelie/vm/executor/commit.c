
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
#include <amelie_value.h>
#include <amelie_set.h>
#include <amelie_output.h>
#include <amelie_executor.h>

hot static void
commit_execute(Commit* self)
{
	auto queue = &self->queue;
	auto batch = &self->batch;
	batch_reset(batch);

	// process aborts (from newest to oldest)
	for (auto it = queue->list_count - 1; it >= 0; it--)
	{
		auto dtr = dtr_queue_at(queue, it);
		if (dtr->abort || dtr->tsn_max >= batch->abort_tsn)
		{
			dtr_set_abort(dtr);
			batch_add_abort(batch, dtr);
		}
	}

	// collect and prepare transactions for commit
	for (auto it = 0; it < queue->list_count; it++)
	{
		auto dtr = dtr_queue_at(queue, it);
		if (! dtr->abort)
			batch_add_commit(batch, dtr);
	}

	// wal write
	//
	// in case of an error abort all prepared transactions
	//
	if (batch->write.list_count)
	{
		auto wal = &self->storage->wal_mgr;
		if (error_catch( wal_mgr_write(wal, &batch->write) ))
			batch_add_abort_all(batch);
	}

	// do group completion
	//
	// update global commit/abort metrics and detach
	// transactions
	//
	executor_detach(self->executor, batch);

	// complete
	batch_wakeup(batch);
}

static void
commit_main(void* arg)
{
	Commit* self = arg;
	auto queue = &self->queue;
	for (;;)
	{
		auto msg = task_recv();
		if (unlikely(msg->id == MSG_STOP))
			break;

		// collect and sort all pending transactions
		dtr_queue_reset(queue);
		dtr_queue_add(queue, (Dtr*)msg);
		while ((msg = task_recv_try()))
			dtr_queue_add(queue, (Dtr*)msg);
		dtr_queue_sort(queue);

		// process group operation
		commit_execute(self);
	}
}

void
commit(Commit* self, Dtr* dtr, Buf* error)
{
	cancel_pause();

	// finilize core transactions and handle errors
	auto error_pending = dispatch_mgr_shutdown(&dtr->dispatch_mgr);
	if (! error)
	{
		// unless transaction is used for replication writer, respect
		// system read-only state
		if (error_pending)
		{
			error = buf_create();
			buf_write_buf(error, error_pending);
		} else
		if (unlikely(dtr->dispatch_mgr.is_write && dtr->program->repl == false &&
		             opt_int_of(&state()->read_only)))
			error = error_create_cstr("system is in read-only mode");
	}
	if (unlikely(error))
	{
		dtr_set_abort(dtr);
		dtr_set_error(dtr, error);
	}
	dtr->tsn_max = dtr->dispatch_mgr.tsn_max;

	// process transaction commit/abort, only if the transaction
	// was registered in executor
	if (dtr_active(dtr))
	{
		task_send(&self->task, &dtr->msg);
		event_wait(&dtr->on_commit, -1);
	}

	cancel_resume();

	if (unlikely(dtr->abort))
	{
		if (dtr->error)
			rethrow_buf(dtr->error);

		error("transaction aborted during commit");
	}
}

void
commit_init(Commit* self, Storage* storage, CoreMgr* core_mgr, Executor* executor)
{
	self->storage  = storage;
	self->core_mgr = core_mgr;
	self->executor = executor;
	batch_init(&self->batch, core_mgr);
	dtr_queue_init(&self->queue);
	task_init(&self->task);
}

void
commit_free(Commit* self)
{
	batch_free(&self->batch);
	dtr_queue_free(&self->queue);
	task_free(&self->task);
}

void
commit_start(Commit* self)
{
	task_create(&self->task, "commit", commit_main, self);
}

void
commit_stop(Commit* self)
{
	// send stop request
	if (task_active(&self->task))
	{
		Msg stop;
		msg_init(&stop, MSG_STOP);
		task_send(&self->task, &stop);
		task_wait(&self->task);
	}
}
