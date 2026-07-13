
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
#include <amelie_db>
#include <amelie_repl>
#include <amelie_value.h>
#include <amelie_set.h>
#include <amelie_output.h>
#include <amelie_commit.h>

hot static inline void
commit_add(GtrQueue* queue, Batch* batch, Gtr* gtr)
{
	auto group = gtr_queue_add(queue, gtr);
	while (gtr_group_pending(group))
	{
		auto gtr = gtr_group_pop(group);
		batch_add(batch, gtr);
	}
}

hot static void
commit_main(void* arg)
{
	Commit* self = arg;

	auto queue = &self->queue;
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

		commit_add(queue, &batch, (Gtr*)msg);
		while ((msg = task_recv_try()))
			commit_add(queue, &batch, (Gtr*)msg);
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
			if (error_catch( db_write(self->db, &batch.write) ))
				batch_abort(&batch);
		}

		// publish cdc events
		if (batch.pending_cdc)
			batch_publish(&batch, self->db->cdc);

		// do group completion
		//
		// update global commit/abort metrics and detach
		// transactions
		//
		auto group_min = gtrs_detach(self->gtrs, &batch);

		// wakeup transactions
		batch_complete(&batch);

		// remove all groups < group_min
		gtr_queue_gc(queue, group_min);
	}
}

void
commit(Commit* self, Gtr* gtr, Buf* error)
{
	cancel_pause();

	// finilize transaction and handle errors
	auto error_pending = dispatches_complete(&gtr->dispatches);
	if (! error)
	{
		// unless transaction is used for replication writer, respect
		// system read-only state
		if (error_pending)
		{
			error = buf_create();
			buf_write_buf(error, error_pending);
		}
	}
	if (unlikely(error))
	{
		gtr_set_abort(gtr);
		gtr_set_error(gtr, error);
	}

	// process transaction commit/abort, only if the transaction
	// was registered in manager
	if (gtr_active(gtr))
	{
		task_send(&self->task, &gtr->msg);
		event_wait(&gtr->on_commit, -1);
	}

	cancel_resume();

	if (unlikely(gtr->abort))
	{
		if (gtr->error)
			rethrow_buf(gtr->error);

		error("transaction aborted during commit");
	}
}

void
commit_init(Commit* self, Db* db, Gtrs* gtrs)
{
	self->db   = db;
	self->gtrs = gtrs;
	gtr_queue_init(&self->queue);
	task_init(&self->task);
}

void
commit_free(Commit* self)
{
	gtr_queue_free(&self->queue);
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

void
commit_sync(Commit* self)
{
	auto recover = self->queue.recover;
	assert(! recover->list);
	recover->id_next = state_lsn() + 1;
}
