
//
// sonata.
//
// Real-Time SQL Database.
//

#include <sonata_runtime.h>
#include <sonata_io.h>
#include <sonata_lib.h>
#include <sonata_data.h>
#include <sonata_config.h>
#include <sonata_user.h>
#include <sonata_http.h>
#include <sonata_client.h>
#include <sonata_server.h>
#include <sonata_row.h>
#include <sonata_transaction.h>
#include <sonata_index.h>
#include <sonata_partition.h>
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
	plan_group_init(&self->group);
	spinlock_init(&self->lock);
}

void
executor_free(Executor* self)
{
	plan_group_free(&self->group);
	spinlock_free(&self->lock);
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

	// send BEGIN to the related nodes
	trx_set_begin(&plan->set);
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

hot static void
executor_begin(Executor* self)
{
	plan_group_reset(&self->group);
	plan_group_create(&self->group, self->router->list_count);

	// collect completed plans and get a list of last
	// completed transactions per node
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

hot static void
executor_end(Executor* self, PlanState state)
{
	// for each node, send last prepared Trx*
	if (state == PLAN_COMMIT)	
	{
		// commit
		trx_set_commit(&self->group.set);
	} else
	{
		// abort all currently active plans
		plan_group_reset(&self->group);
		plan_group_create(&self->group, self->router->list_count);

		// add plans to the list and collect a list of last
		// completed transactions per node
		list_foreach(&self->list)
		{
			auto plan = list_at(Plan, link);
			plan_group_add(&self->group, plan);
		}

		trx_set_abort(&self->group.set);
	}

	// finilize plans
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
}

hot static void
executor_end_lock(Executor* self, PlanState state)
{
	spinlock_lock(&self->lock);
	guard(spinlock_unlock, &self->lock);
	executor_end(self, state);
}

hot void
executor_wal_write(Executor* self)
{
	auto wal = &self->db->wal;
	auto wal_batch = &self->group.wal_batch;

	list_foreach(&self->group.list)
	{
		auto plan = list_at(Plan, link_group);
		auto set = &plan->set;

		// wal write (disabled during recovery)
		wal_batch_reset(wal_batch);
		wal_batch_begin(wal_batch, 0);
		for (int i = 0; i < set->set_size; i++)
		{
			auto trx = trx_set_get(set, i);
			if (trx == NULL)
				continue;
			wal_batch_add(wal_batch, &trx->trx.log.log_set);
		}
		if (wal_batch->header.count > 0)
		{
			// unless plan is used for replication writer, respect system
			// read-only state
			if (!plan->repl && var_int_of(&config()->read_only))
				error("system is in read-only mode");
			wal_write(wal, wal_batch);
		}
	}
}

hot void
executor_commit(Executor* self, Plan* plan)
{
	for (;;)
	{
		spinlock_lock(&self->lock);
		guard(spinlock_unlock, &self->lock);

		switch (plan->state) {
		case PLAN_NONE:
			// non-distributed plan executed on coordinator
			if (plan->error)
				msg_error_throw(plan->error);
			return;
		case PLAN_COMMIT:
			// commited by leader
			return;
		case PLAN_ABORT:
			// aborted by leader
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
			// wait to become leader or wakeup by leader
			unguard();
			spinlock_unlock(&self->lock);

			condition_wait(plan->on_commit, -1);
			continue;
		}

		// leader error
		if (plan->error)
		{
			executor_end(self, PLAN_ABORT);
			msg_error_throw(plan->error);
		}

		// wal write and group commit

		// get a list of completed plans (one or more) and a list of
		// last executed transactions per node
		executor_begin(self);

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
			executor_end_lock(self, PLAN_ABORT);
			rethrow();
		}

		// COMMIT
		executor_end_lock(self, PLAN_COMMIT);
		break;
	}
}
