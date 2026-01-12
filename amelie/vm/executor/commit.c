
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
#include <amelie_value.h>
#include <amelie_set.h>
#include <amelie_output.h>
#include <amelie_executor.h>

hot static inline void
commit_add(DtrQueue* queue, Batch* batch, Dtr* dtr)
{
	auto group = dtr_queue_add(queue, dtr);
	while (dtr_group_pending(group))
	{
		auto dtr = dtr_group_pop(group);
		batch_add(batch, dtr);
	}
}

static void
commit_main(void* arg)
{
	Commit* self = arg;

	DtrQueue queue;
	dtr_queue_init(&queue);
	defer(dtr_queue_free, &queue);

	Batch batch;
	batch_init(&batch);
	defer(batch_free, &batch);
	for (;;)
	{
		auto msg = task_recv();
		if (unlikely(msg->id == MSG_STOP))
			break;

		// collect prepared transactions
		batch_reset(&batch);
		commit_add(&queue, &batch, (Dtr*)msg);
		while ((msg = task_recv_try()))
			commit_add(&queue, &batch, (Dtr*)msg);
		if (batch_empty(&batch))
			continue;

		// prepare transactions for commit and handle aborts
		batch_process(&batch);

		// wal write
		//
		// in case of an error abort all prepared transactions
		//
		if (batch.write.list_count)
		{
			auto wal = &self->storage->wal_mgr;
			if (error_catch( wal_mgr_write(wal, &batch.write) ))
				batch_abort(&batch);
		}

		// do group completion
		//
		// update global commit/abort metrics and detach
		// transactions
		//
		auto group_min = executor_detach(self->executor, &batch);

		// wakeup and unlock prepared transaction
		batch_complete(&batch, self->storage);

		// remove all groups < group_min
		dtr_queue_gc(&queue, group_min);
	}
}

void
commit(Commit* self, Dtr* dtr, Buf* error)
{
	cancel_pause();

	// finilize transaction and handle errors
	bool write;
	auto error_pending = dispatch_mgr_complete(&dtr->dispatch_mgr, &write);
	if (! error)
	{
		// unless transaction is used for replication writer, respect
		// system read-only state
		if (error_pending)
		{
			error = buf_create();
			buf_write_buf(error, error_pending);
		} else
		if (unlikely(write && dtr->program->repl == false &&
		             opt_int_of(&state()->read_only)))
			error = error_create_cstr("system is in read-only mode");
	}
	if (unlikely(error))
	{
		dtr_set_abort(dtr);
		dtr_set_error(dtr, error);
	}

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
commit_init(Commit* self, Storage* storage, Executor* executor)
{
	self->storage  = storage;
	self->executor = executor;
	task_init(&self->task);
}

void
commit_free(Commit* self)
{
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
