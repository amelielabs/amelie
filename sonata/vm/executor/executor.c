
//
// sonata.
//
// SQL Database for JSON.
//

#include <sonata_runtime.h>
#include <sonata_io.h>
#include <sonata_lib.h>
#include <sonata_data.h>
#include <sonata_config.h>
#include <sonata_auth.h>
#include <sonata_http.h>
#include <sonata_client.h>
#include <sonata_server.h>
#include <sonata_def.h>
#include <sonata_transaction.h>
#include <sonata_index.h>
#include <sonata_storage.h>
#include <sonata_wal.h>
#include <sonata_db.h>
#include <sonata_value.h>
#include <sonata_aggr.h>
#include <sonata_executor.h>

void
executor_init(Executor* self, Db* db, Router* router)
{
	self->db         = db;
	self->router     = router;
	self->list_count = 0;
	list_init(&self->list);
	plan_group_init(&self->group, router);
	spinlock_init(&self->lock);
}

void
executor_free(Executor* self)
{
	plan_group_free(&self->group);
	spinlock_free(&self->lock);
}

void
executor_create(Executor* self)
{
	plan_group_create(&self->group);
}

hot void
executor_send(Executor* self, Plan* plan, int stmt, ReqList* list)
{
	// create transactions and put requests in the queues
	plan_send(plan, stmt, list);
	if (plan->state == PLAN_ACTIVE)
		return;

	spinlock_lock(&self->lock);
	// todo: move msg creation out of lock?

	// add plan to the executor list
	plan_set_state(plan, PLAN_ACTIVE);
	list_append(&self->list, &plan->link);
	self->list_count++;

	// send BEGIN to the related shards
	plan_begin(plan);
	spinlock_unlock(&self->lock);
}

hot void
executor_recv(Executor* self, Plan* plan, int stmt)
{
	unused(self);
	// OK or ABORT
	plan_recv(plan, stmt);
}

void
executor_complete(Executor* self, Plan* plan)
{
	spinlock_lock(&self->lock);
	if (plan->state == PLAN_ACTIVE)
		plan_set_state(plan, PLAN_COMPLETE);
	spinlock_unlock(&self->lock);
}

static void
executor_abort(Executor* self)
{
	// executed under the lock

	// abort all active plans
	plan_group_reset(&self->group);

	// add plans to the list and collect a list of last
	// completed transactions per shard
	list_foreach(&self->list)
	{
		auto plan = list_at(Plan, link);
		plan_group_add(&self->group, plan);
	}
}

static inline void
executor_commit_start(Executor* self)
{
	plan_group_reset(&self->group);

	// collect completed plans and get a list of last
	// completed transactions per shard
	list_foreach(&self->list)
	{
		auto plan = list_at(Plan, link);
		if (plan->state != PLAN_COMPLETE)
			break;
		if (plan->error)
			break;
		plan_group_add(&self->group, plan);
	}
}

hot void
executor_commit_end(Executor* self, PlanState state)
{
	// for each shard, send last prepared Trx*
	if (state == PLAN_COMMIT)	
		plan_group_commit(&self->group);
	else
		plan_group_abort(&self->group);

	// finilize plans
	spinlock_lock(&self->lock);

	list_foreach(&self->group.list)
	{
		auto plan = list_at(Plan, link_group);

		// update plan state
		plan_set_state(plan, state);

		// remove plan from the executor
		list_unlink(&plan->link);
		self->list_count--;

		// wakeup
		condition_signal(plan->on_commit);
	}

	// wakeup next leader, if any
	if (self->list_count > 0)
	{
		auto leader = container_of(self->list.next, Plan, link);
		condition_signal(leader->on_commit);
	}

	spinlock_unlock(&self->lock);
}

hot void
executor_wal_write(Executor* self)
{
	auto wal = &self->db->wal;
	auto wal_enabled = var_int_of(&config()->wal);
	auto wal_batch = &self->group.wal_batch;

	list_foreach(&self->group.list)
	{
		auto plan = list_at(Plan, link_group);

		// get next lsn
		uint64_t lsn = config_lsn() + 1;
		wal_batch_reset(wal_batch);
		wal_batch_begin(wal_batch, lsn);

		auto router = self->router;
		for (int i = 0; i < router->set_size; i++)
		{
			auto trx = dispatch_get(&plan->dispatch, i);
			if (trx == NULL)
				continue;
			transaction_set_lsn(&trx->trx, lsn);
			if (! wal_enabled)
				continue;
			wal_batch_add(wal_batch, &trx->trx.log.log_set);
		}

		// wal write
		if (wal_enabled && wal_batch->header.count > 0)
		{
			auto rotate_ready = wal_write(wal, wal_batch);
			if (rotate_ready)
				wal_rotate(wal, 0);
		}

		// update lsn globally
		config_lsn_set(lsn);
	}
}

hot void
executor_commit(Executor* self, Plan* plan)
{
	for (;;)
	{
		spinlock_lock(&self->lock);

		switch (plan->state) {
		case PLAN_NONE:
			// non-distributed plan executed on coordinator
			spinlock_unlock(&self->lock);
			if (plan->error)
				msg_error_rethrow(plan->error);
			return;
		case PLAN_COMMIT:
			// commited by leader
			spinlock_unlock(&self->lock);
			return;
		case PLAN_ABORT:
			// aborted by leader
			spinlock_unlock(&self->lock);
			error("transaction conflict, abort");
			break;
		case PLAN_COMPLETE:
			break;
		default:
			abort();
			break;
		}

		// if current plan is commit leader (first plan in the list)
		if (! list_is_first(&self->list, &plan->link))
		{
			// wait for leader
			spinlock_unlock(&self->lock);
			condition_wait(plan->on_commit, -1);
			continue;
		}

		// leader
		if (plan->error)
		{
			executor_abort(self);
			spinlock_unlock(&self->lock);

			executor_commit_end(self, PLAN_ABORT);
			msg_error_rethrow(plan->error);
		}

		// wal write and group commit

		// get a list of completed plans (one or more) and a list of
		// last executed transactions per shard
		executor_commit_start(self);

		spinlock_unlock(&self->lock);

		// wal write
		Exception e;
		if (enter(&e))
		{
			executor_wal_write(self);
		}
		if (leave(&e))
		{
			spinlock_lock(&self->lock);
			executor_abort(self);
			spinlock_unlock(&self->lock);

			executor_commit_end(self, PLAN_ABORT);
			rethrow();
		}

		// COMMIT
		executor_commit_end(self, PLAN_COMMIT);
		break;
	}
}
