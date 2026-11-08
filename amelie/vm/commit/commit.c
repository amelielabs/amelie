
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
#include <amelie_heap>
#include <amelie_storage>
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

		// wal write
		if (batch.write.list_count)
			db_write(self->db, &batch.write);

		// wakeup transactions
		batch_complete(&batch);
	}
}

void
commit(Commit* self, Gtr* gtr)
{
	cancel_pause();

	auto user = user_of(gtr->tr.user_);
	user->commit++;

	task_send(&self->task, &gtr->msg);
	event_wait(&gtr->on_commit, -1);

	user->commit--;
	cancel_resume();
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
