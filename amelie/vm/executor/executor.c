
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
#include <amelie_value.h>
#include <amelie_set.h>
#include <amelie_content.h>
#include <amelie_executor.h>

void
executor_init(Executor* self, Db* db)
{
	self->id         = 0;
	self->id_commit  = 0;
	self->list_count = 0;
	self->db         = db;
	prepare_init(&self->prepare);
	list_init(&self->list);
	spinlock_init(&self->lock);
}

void
executor_free(Executor* self)
{
	spinlock_free(&self->lock);
}

hot void
executor_send(Executor* self, Dtr* dtr, int step, ReqList* list)
{
	// add requests to the dispatch step,
	// start local transactions and queue requests for execution
	dtr_send(dtr, step, list);

	if (step == 0)
	{
		spinlock_lock(&self->lock);

		// register transaction
		dtr_set_state(dtr, DTR_BEGIN);
		dtr->id = self->id++;
		dtr->id_commit = self->id_commit;

		list_append(&self->list, &dtr->link);
		self->list_count++;

		// send for execution
		auto dispatch = &dtr->dispatch;
		for (auto order = 0; order < dispatch->core_mgr->cores_count; order++)
		{
			auto ctr = &dispatch->cores[order];
			if (ctr->state == CTR_ACTIVE)
				core_add(ctr->core, ctr);
		}

		spinlock_unlock(&self->lock);
	}
}

hot void
executor_recv(Executor* self, Dtr* dtr, int step)
{
	unused(self);
	dtr_recv(dtr, step);
}

hot static void
executor_prepare(Executor* self, bool abort)
{
	auto prepare = &self->prepare;
	prepare_reset(prepare);

	if (unlikely(abort))
	{
		// add all currently active transactions for abort
		list_foreach(&self->list)
		{
			auto dtr = list_at(Dtr, link);
			prepare_add(prepare, dtr);
		}
	} else
	{
		// collect a list of prepared distributed transactions
		list_foreach(&self->list)
		{
			auto dtr = list_at(Dtr, link);
			if (dtr->state != DTR_PREPARE)
				break;
			prepare_add(prepare, dtr);
		}
	}
}

hot static void
executor_end(Executor* self, DtrState state)
{
	auto prepare = &self->prepare;
	if (unlikely(state == DTR_ABORT))
	{
		executor_prepare(self, true);
		//
		// ABORT
		//
		// reset current id as min active transaction, this will
		// force to abort prepared transaction by a next
		// transaction.
		//
		self->id = prepare->id_min;
	} else
	{
		//
		// COMMIT
		//
		// set current id (id for a next transaction) higher than current one,
		// this will prevent abort by a next transaction.
		//
		// set current commit id as a current id, this will commit prepared
		// transactions by a next transaction.
		//
		self->id        = prepare->id_max + 1;
		self->id_commit = prepare->id_max;
	}

	// finilize distributed transactions
	list_foreach(&prepare->list)
	{
		auto dtr = list_at(Dtr, link_prepare);

		// update state
		auto tr_state = dtr->state;
		dtr_set_state(dtr, state);

		// remove transaction from the executor
		list_unlink(&dtr->link);
		self->list_count--;

		// wakeup
		if (tr_state == DTR_PREPARE ||
		    tr_state == DTR_ERROR)
			event_signal(&dtr->on_commit);
	}

	// wakeup next commit leader, if any
	if (self->list_count > 0)
	{
		auto leader = container_of(self->list.next, Dtr, link);
		if (leader->state == DTR_PREPARE ||
		    leader->state == DTR_ERROR)
			event_signal(&leader->on_commit);
	}
}

hot static void
executor_wal_write(Executor* self)
{
	// prepare a list of a prepared wal records
	auto prepare = &self->prepare;
	auto write_list = &prepare->write;
	list_foreach(&prepare->list)
	{
		auto dtr      = list_at(Dtr, link_prepare);
		auto dispatch = &dtr->dispatch;
		auto write    = &dtr->write;
		write_reset(write);
		write_begin(write);
		for (int i = 0; i < dispatch->core_mgr->cores_count; i++)
		{
			auto ctr = &dispatch->cores[i];
			if (ctr->state == CTR_NONE)
				continue;
			assert(ctr->tr);
			write_add(write, &ctr->tr->log.write_log);
		}
		if (write->header.count > 0)
		{
			// unless transaction is used for replication writer, respect
			// system read-only state
			if (!dtr->program->repl && var_int_of(&state()->read_only))
				error("system is in read-only mode");
			write_list_add(write_list, write);
		}
	}

	// do atomical wal write
	wal_mgr_write(&self->db->wal_mgr, write_list);
}

hot void
executor_commit(Executor* self, Dtr* dtr, Buf* error)
{
	// finilize dispatch state after error
	dispatch_shutdown(&dtr->dispatch);

	for (;;)
	{
		spinlock_lock(&self->lock);

		switch (dtr->state) {
		case DTR_COMMIT:
			// commited by leader
			spinlock_unlock(&self->lock);
			return;
		case DTR_ABORT:
			// aborted by leader
			spinlock_unlock(&self->lock);
			error("transaction conflict, abort");
			return;
		case DTR_NONE:
			// non-distributed transaction
			spinlock_unlock(&self->lock);
			if (error)
			{
				dtr_set_error(dtr, error);
				msg_error_rethrow(error);
			}
			return;
		case DTR_BEGIN:
			if (unlikely(error))
			{
				dtr_set_state(dtr, DTR_ERROR);
				dtr_set_error(dtr, error);
			} else {
				dtr_set_state(dtr, DTR_PREPARE);
			}
			break;
		case DTR_PREPARE:
		case DTR_ERROR:
			// retry after wakeup
			break;
		default:
			abort();
			break;
		}

		// commit leader is the first transaction in the list
		if (! list_is_first(&self->list, &dtr->link))
		{
			// wait to become leader or wakeup by leader
			spinlock_unlock(&self->lock);

			event_wait(&dtr->on_commit, -1);
			continue;
		}

		// handle leader error
		if (dtr->state == DTR_ERROR)
		{
			executor_end(self, DTR_ABORT);
			spinlock_unlock(&self->lock);
			msg_error_throw(dtr->error);
		}

		// prepare for group commit and wal write

		// get a list of completed distributed transactions (one or more) and
		// a list of last executed transactions per partition
		executor_prepare(self, false);
		spinlock_unlock(&self->lock);

		// wal write
		DtrState state = DTR_COMMIT;
		if (unlikely(error_catch( executor_wal_write(self) )))
			state = DTR_ABORT;

		// process COMMIT or ABORT
		spinlock_lock(&self->lock);
		executor_end(self, state);
		spinlock_unlock(&self->lock);
		if (state == DTR_ABORT)
			rethrow();

		break;
	}
}
