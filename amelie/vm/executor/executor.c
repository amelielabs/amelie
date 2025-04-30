
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
executor_init(Executor* self, Db* db, CoreMgr* core_mgr)
{
	self->id              = 1;
	self->list_count      = 0;
	self->list_wait_count = 0;
	self->core_mgr        = core_mgr;
	self->db              = db;
	prepare_init(&self->prepare);
	list_init(&self->list);
	list_init(&self->list_wait);
	spinlock_init(&self->lock);
}

void
executor_free(Executor* self)
{
	spinlock_free(&self->lock);
	prepare_free(&self->prepare);
}

hot static inline void
executor_lock(Executor* self, Dtr* dtr)
{
restart:
	list_foreach(&self->list)
	{
		auto ref = list_at(Dtr, link);
		if (access_try(&ref->program->access, &dtr->program->access))
			continue;

		// wait for conflicting transaction last send
		list_append(&self->list_wait, &dtr->link_access);
		self->list_wait_count++;
		spinlock_unlock(&self->lock);

		event_wait(&dtr->on_access, -1);

		spinlock_lock(&self->lock);
		list_unlink(&dtr->link_access);
		self->list_wait_count--;
		goto restart;
	}
}

hot void
executor_send(Executor* self, Dtr* dtr, ReqList* list)
{
	auto dispatch_mgr = &dtr->dispatch_mgr;
	auto first = dispatch_mgr_is_first(dispatch_mgr);

	// start local transactions and queue requests for execution
	dtr_send(dtr, list);

	// register transaction and begin execution
	if (first)
	{
		spinlock_lock(&self->lock);

		// process exclusive transaction locking
		executor_lock(self, dtr);

		// register transaction
		dtr_set_state(dtr, DTR_BEGIN);
		dtr->id = self->id++;
		list_append(&self->list, &dtr->link);
		self->list_count++;

		// begin execution
		for (auto order = 0; order < dispatch_mgr->ctrs_count; order++)
		{
			auto ctr = dispatch_mgr_ctr(dispatch_mgr, order);
			if (ctr->state == CTR_ACTIVE)
				core_add(ctr->core, ctr);
		}

		spinlock_unlock(&self->lock);
	}
}

hot void
executor_recv(Executor* self, Dtr* dtr)
{
	unused(self);
	dtr_recv(dtr);
}

hot static void
executor_prepare(Executor* self, bool abort)
{
	auto prepare = &self->prepare;
	prepare_reset(prepare, self->core_mgr);

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

		// abort each involved core
		auto cores = (Core**)prepare->cores.start;
		for (auto order = 0; order < self->core_mgr->cores_count; order++)
			if (cores[order])
				core_abort(cores[order]);

	} else
	{
		// commit each involved core
		auto cores = (Core**)prepare->cores.start;
		for (auto order = 0; order < self->core_mgr->cores_count; order++)
			if (cores[order])
				core_commit(cores[order], prepare->id_max);
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

	// wakeup access waiters
	if (self->list_wait_count > 0)
	{
		list_foreach_safe(&self->list_wait)
		{
			auto dtr = list_at(Dtr, link_access);
			event_signal(&dtr->on_access);
		}
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
		auto dtr          = list_at(Dtr, link_prepare);
		auto dispatch_mgr = &dtr->dispatch_mgr;
		auto write        = &dtr->write;
		write_reset(write);
		write_begin(write);
		for (int i = 0; i < dispatch_mgr->ctrs_count; i++)
		{
			auto ctr = dispatch_mgr_ctr(dispatch_mgr, i);
			if (ctr->state == CTR_NONE)
				continue;
			if (ctr->tr == NULL || tr_read_only(ctr->tr))
				continue;
			write_add(write, &ctr->tr->log.write_log);
		}
		if (write->header.count > 0)
		{
			// unless transaction is used for replication writer, respect
			// system read-only state
			if (!dtr->program->repl && opt_int_of(&state()->read_only))
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
	// finilize any core transactions that are still active,
	// this can happen because of error or by premature
	// return statement
	dispatch_mgr_shutdown(&dtr->dispatch_mgr);

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
		// a list of last executed transactions per core
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
