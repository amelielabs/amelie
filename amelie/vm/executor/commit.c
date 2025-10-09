
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

#include <amelie_base.h>
#include <amelie_os.h>
#include <amelie_lib.h>
#include <amelie_json.h>
#include <amelie_runtime.h>
#include <amelie_user.h>
#include <amelie_http.h>
#include <amelie_client.h>
#include <amelie_server.h>
#include <amelie_row.h>
#include <amelie_heap.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_catalog.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_value.h>
#include <amelie_set.h>
#include <amelie_content.h>
#include <amelie_executor.h>

static void
commit_complete(Commit* self, bool abort)
{
	auto core_mgr = self->core_mgr;
	auto cores = (Core**)self->prepare.cores.start;

	if (unlikely(abort))
	{
		// abort each involved core
		for (auto order = 0; order < core_mgr->cores_count; order++)
			if (cores[order])
				core_abort(cores[order]);
	} else
	{
		// commit each involved core
		for (auto order = 0; order < core_mgr->cores_count; order++)
			if (cores[order])
				core_commit(cores[order], self->prepare.id_max);
	}

	// remove transactions from executor and handle abort
	executor_detach(self->executor, self->prepare.list, abort);

	// signal completion
	auto dtr = self->prepare.list;
	while (dtr)
	{
		auto next = dtr->link_queue;
		event_signal(&dtr->on_commit);
		dtr = next;
	}
}

static void
commit_process(Commit* self, DtrQueue* queue)
{
	// get a list of completed distributed transactions (one or more) and
	// a list of last executed transactions per core
	auto wal = &self->db->wal_mgr;
	auto prepare = &self->prepare;
	prepare_reset(prepare, self->core_mgr);
	for (;;)
	{
		// get next transaction (in sync and ordered by id)
		auto dtr = dtr_queue_next(queue);
		if (! dtr)
			break;
		prepare_add(prepare, dtr);

		bool abort;
		if (likely(! dtr->abort))
		{
			// GROUP COMMIT
			//
			abort = false;
			while ((dtr = dtr_queue_peek(queue)))
			{
				// transactions with error will remain in the queue
				// for the next iteration
				if (dtr->abort)
					break;
				dtr_queue_pop(queue);
				prepare_add(prepare, dtr);
			}

			// wal write
			if (prepare->write.list_count)
				abort = error_catch( wal_mgr_write(wal, &prepare->write) );
		} else
		{
			// GROUP ABORT
			//
			// transaction error or forced abort
			//
			// aborted transactions are always first in the queue
			//
			abort = true;
			while ((dtr = dtr_queue_next(queue)))
				prepare_add(prepare, dtr);
		}

		// commit/abort
		commit_complete(self, abort);
	}
}

static void
commit_main(void* arg)
{
	Commit* self = arg;

	DtrQueue queue;
	dtr_queue_init(&queue);
	for (;;)
	{
		auto msg = task_recv();
		if (unlikely(msg->id == MSG_STOP))
			break;

		// order incoming transactions by id
		dtr_queue_add(&queue, (Dtr*)msg);
		while ((msg = task_recv_try()))
		{
			assert(msg->id == MSG_DTR);
			dtr_queue_add(&queue, (Dtr*)msg);
		}

		// if queue has ready transactions
		if (dtr_queue_peek(&queue))
			commit_process(self, &queue);
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
commit_init(Commit* self, Db* db, CoreMgr* core_mgr, Executor* executor)
{
	self->db       = db;
	self->core_mgr = core_mgr;
	self->executor = executor;
	prepare_init(&self->prepare);
	task_init(&self->task);
}

void
commit_free(Commit* self)
{
	prepare_free(&self->prepare);
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
