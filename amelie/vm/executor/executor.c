
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
executor_init(Executor* self, Db* db, Router* router)
{
	self->db              = db;
	self->router          = router;
	self->list_count      = 0;
	self->list_wait_count = 0;
	list_init(&self->list);
	list_init(&self->list_wait);
	prepare_init(&self->prepare);
	spinlock_init(&self->lock);
}

void
executor_free(Executor* self)
{
	prepare_free(&self->prepare);
	spinlock_free(&self->lock);
}

hot static inline void
executor_lock(Executor* self, Dtr* dtr)
{
restart:
	list_foreach(&self->list)
	{
		auto ref = list_at(Dtr, link);
		if (access_try(ref->program->access, dtr->program->access))
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
executor_send(Executor* self, Dtr* dtr, int stmt, ReqList* list)
{
	auto first = !dtr->dispatch.sent;

	// put requests into pipes per backend
	dtr_send(dtr, stmt, list);

	// register transaction and begin execution
	if (first)
	{
		spinlock_lock(&self->lock);

		// process transaction locking
		executor_lock(self, dtr);

		dtr_set_state(dtr, DTR_BEGIN);

		// add transaction to the executor list
		list_append(&self->list, &dtr->link);
		self->list_count++;

		// send BEGIN to the related backends
		pipe_set_begin(&dtr->set);
		spinlock_unlock(&self->lock);
	}
}

hot void
executor_recv(Executor* self, Dtr* dtr, int stmt)
{
	unused(self);
	// OK or ABORT
	dtr_recv(dtr, stmt);
}

hot static void
executor_prepare(Executor* self, bool abort)
{
	auto prepare = &self->prepare;
	prepare_reset(prepare);
	prepare_prepare(prepare, self->router->list_count);

	// get a list of last completed local transactions per backend
	if (unlikely(abort))
	{
		// abort all currently active transactions
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
	// for each backend, send last prepared Trx*
	auto prepare = &self->prepare;
	if (state == DTR_COMMIT)
	{
		// RPC_COMMIT
		pipe_set_commit(&prepare->set);
	} else
	{
		executor_prepare(self, true);

		// RPC_ABORT
		pipe_set_abort(&prepare->set);
	}

	// finilize transactions
	list_foreach(&prepare->list)
	{
		auto dtr = list_at(Dtr, link_prepare);

		// update state
		auto dtr_state = dtr->state;
		dtr_set_state(dtr, state);

		// remove transaction from the executor
		list_unlink(&dtr->link);
		self->list_count--;

		// wakeup
		if (dtr_state == DTR_PREPARE ||
		    dtr_state == DTR_ERROR)
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
	// prepare a list of a finilized wal records
	auto prepare = &self->prepare;
	auto write_list = &prepare->write_list;
	list_foreach(&prepare->list)
	{
		auto dtr   = list_at(Dtr, link_prepare);
		auto set   = &dtr->set;
		auto write = &dtr->write;
		write_reset(write);
		write_begin(write);
		for (int i = 0; i < set->set_size; i++)
		{
			auto pipe = pipe_set_get(set, i);
			if (pipe == NULL)
				continue;
			assert(pipe->state == PIPE_CLOSE);
			assert(pipe->tr);
			if (tr_read_only(pipe->tr))
				continue;
			write_add(write, &pipe->tr->log.write_log);
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
	// shutdown pipes if there are any left open,
	// this can happen because of error or by premature
	// return statement
	dtr_shutdown(dtr);

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
		// a list of last executed transactions per backend
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
