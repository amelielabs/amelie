
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

hot static void
commit_main(void* arg)
{
	Commit* self = arg;

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
		batch_add(&batch, (Gtr*)msg);
		while ((msg = task_recv_try()))
			batch_add(&batch, (Gtr*)msg);
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

		// update commit/abort metrics and wakeup
		// transactions
		batch_complete(&batch);
	}
}

void
commit(Commit* self, Gtr* gtr, Buf* error)
{
	cancel_pause();

	// note: dispatch is completed
	if (unlikely(error))
	{
		gtr_set_abort(gtr);
		gtr_set_error(gtr, error);
	}

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

		error("transaction aborted");
	}
}

void
commit_init(Commit* self, Db* db)
{
	self->db = db;
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
