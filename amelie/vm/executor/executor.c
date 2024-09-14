
//
// amelie.
//
// Real-Time SQL Database.
//

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_data.h>
#include <amelie_config.h>
#include <amelie_user.h>
#include <amelie_http.h>
#include <amelie_client.h>
#include <amelie_server.h>
#include <amelie_row.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_value.h>
#include <amelie_aggr.h>
#include <amelie_executor.h>

void
executor_init(Executor* self, Db* db, Router* router)
{
	self->db         = db;
	self->router     = router;
	self->list_count = 0;
	list_init(&self->list);
	commit_init(&self->commit);
	spinlock_init(&self->lock);
}

void
executor_free(Executor* self)
{
	commit_free(&self->commit);
	spinlock_free(&self->lock);
}

hot void
executor_send(Executor* self, Dtr* tr, int stmt, ReqList* list)
{
	auto first = !tr->dispatch.sent;

	// put requests into pipes per nodes
	dtr_send(tr, stmt, list);

	// register transaction and begin execution
	if (first)
	{
		spinlock_lock(&self->lock);
		guard(spinlock_unlock, &self->lock);

		dtr_set_state(tr, DTR_BEGIN);

		// add transaction to the executor list
		list_append(&self->list, &tr->link);
		self->list_count++;

		// send BEGIN to the related nodes
		pipe_set_begin(&tr->set);
	}
}

hot void
executor_recv(Executor* self, Dtr* tr, int stmt)
{
	unused(self);
	// OK or ABORT
	dtr_recv(tr, stmt);
}

hot static void
executor_prepare(Executor* self, bool abort)
{
	auto commit = &self->commit;
	commit_reset(commit);
	commit_prepare(commit, self->router->list_count);

	// get a list of last completed local transactions per node
	if (unlikely(abort))
	{
		// abort all currently active transactions
		list_foreach(&self->list)
		{
			auto tr = list_at(Dtr, link);
			commit_add(commit, tr);
		}
	} else
	{
		// collect a list of prepared distributed transactions
		list_foreach(&self->list)
		{
			auto tr = list_at(Dtr, link);
			if (tr->state != DTR_PREPARE)
				break;
			commit_add(commit, tr);
		}
	}
}

hot static void
executor_end(Executor* self, DtrState state)
{
	auto commit = &self->commit;

	// for each node, send last prepared Trx*
	if (state == DTR_COMMIT)
	{
		// RPC_COMMIT
		pipe_set_commit(&commit->set);
	} else
	{
		executor_prepare(self, true);

		// RPC_ABORT
		pipe_set_abort(&commit->set);
	}

	// finilize transactions
	list_foreach(&commit->list)
	{
		auto tr = list_at(Dtr, link_commit);

		// update state
		auto tr_state = tr->state;
		dtr_set_state(tr, state);

		// remove transaction from the executor
		list_unlink(&tr->link);
		self->list_count--;

		// wakeup
		if (tr_state == DTR_PREPARE ||
		    tr_state == DTR_ERROR)
			cond_signal(&tr->on_commit, 0);
	}

	// wakeup next commit leader, if any
	if (self->list_count > 0)
	{
		auto leader = container_of(self->list.next, Dtr, link);
		if (leader->state == DTR_PREPARE ||
		    leader->state == DTR_ERROR)
			cond_signal(&leader->on_commit, 0);
	}
}

hot static void
executor_end_lock(Executor* self, DtrState state)
{
	spinlock_lock(&self->lock);
	guard(spinlock_unlock, &self->lock);
	executor_end(self, state);
}

hot void
executor_wal_write(Executor* self)
{
	auto commit = &self->commit;
	auto wal_batch = &commit->wal_batch;
	auto wal = &self->db->wal;

	list_foreach(&commit->list)
	{
		auto tr  = list_at(Dtr, link_commit);
		auto set = &tr->set;

		// wal write (disabled during recovery)
		wal_batch_reset(wal_batch);
		wal_batch_begin(wal_batch, 0);
		for (int i = 0; i < set->set_size; i++)
		{
			auto pipe = pipe_set_get(set, i);
			if (pipe == NULL)
				continue;
			assert(pipe->state == PIPE_CLOSE);
			assert(pipe->tr);
			wal_batch_add(wal_batch, &pipe->tr->log.log_set);
		}
		if (wal_batch->header.count > 0)
		{
			// unless transaction is used for replication writer, respect
			// system read-only state
			if (!tr->repl && var_int_of(&config()->read_only))
				error("system is in read-only mode");
			wal_write(wal, wal_batch);
		}
	}
}

hot void
executor_commit(Executor* self, Dtr* tr, Buf* error)
{
	if (error)
		dtr_shutdown(tr);

	for (;;)
	{
		spinlock_lock(&self->lock);
		guard(spinlock_unlock, &self->lock);

		switch (tr->state) {
		case DTR_COMMIT:
			// commited by leader
			return;
		case DTR_ABORT:
			// aborted by leader
			error("transaction conflict, abort");
			return;
		case DTR_NONE:
			// non-distributed transaction
			if (error)
				msg_error_rethrow(error);
			return;
		case DTR_BEGIN:
			if (unlikely(error))
			{
				dtr_set_state(tr, DTR_ERROR);
				dtr_set_error(tr, error);
			} else {
				dtr_set_state(tr, DTR_PREPARE);
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

		// if current tr is commit leader (first tr in the list)
		if (! list_is_first(&self->list, &tr->link))
		{
			// wait to become leader or wakeup by leader
			unguard();
			spinlock_unlock(&self->lock);

			cond_wait(&tr->on_commit);
			continue;
		}

		// leader error
		if (tr->state == DTR_ERROR)
		{
			executor_end(self, DTR_ABORT);
			msg_error_throw(tr->error);
		}

		// wal write and group commit

		// get a list of completed distributed transactionn (one or more) and
		// a list of last executed transactions per node
		executor_prepare(self, false);

		unguard();
		spinlock_unlock(&self->lock);

		// wal write
		Exception e;
		if (enter(&e))
		{
			executor_wal_write(self);
		}
		if (leave(&e))
		{
			// ABORT
			executor_end_lock(self, DTR_ABORT);
			rethrow();
		}

		// COMMIT
		executor_end_lock(self, DTR_COMMIT);
		break;
	}
}
