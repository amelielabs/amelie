#pragma once

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

typedef struct Executor Executor;

struct Executor
{
	Spinlock  lock;
	List      list;
	List      list_wait;
	Consensus consensus;
};

static inline void
executor_init(Executor* self)
{
	spinlock_init(&self->lock);
	list_init(&self->list);
	list_init(&self->list_wait);
	consensus_init(&self->consensus);
}

static inline void
executor_free(Executor* self)
{
	spinlock_free(&self->lock);
}

static inline void
executor_prepare(Executor* self)
{
	self->consensus.commit = state_tsn();
	self->consensus.abort  = 0;
}

hot static inline void
executor_attach_lock(Executor* self, Dtr* dtr)
{
restart:
	list_foreach(&self->list)
	{
		auto ref = list_at(Dtr, link);
		if (access_try(&ref->program->access, &dtr->program->access))
			continue;

		// wait for conflicting transaction completion
		list_append(&self->list_wait, &dtr->link_access);
		spinlock_unlock(&self->lock);

		event_wait(&dtr->on_access, -1);

		spinlock_lock(&self->lock);
		list_unlink(&dtr->link_access);
		goto restart;
	}
}

hot static inline void
executor_attach(Executor* self, Dtr* dtr, Dispatch* dispatch)
{
	spinlock_lock(&self->lock);

	// process exclusive transaction locking
	executor_attach_lock(self, dtr);

	// register transaction and set global metrics
	dtr->tsn = state_tsn_next();
	dtr->consensus = self->consensus;
	list_append(&self->list, &dtr->link);

	// begin execution
	dispatch_mgr_send(&dtr->dispatch_mgr, dispatch);

	spinlock_unlock(&self->lock);
}

hot static inline void
executor_send(Executor* self, Dtr* dtr, Dispatch* dispatch)
{
	auto mgr = &dtr->dispatch_mgr;
	dispatch_prepare(dispatch);

	// for the first statement
	//
	// register transaction and send requests under the same lock,
	// this ensure serialization and prevents single-statements
	// conflicts.
	//
	auto first = dispatch_mgr_is_first(mgr);
	if (first)
	{
		executor_attach(self, dtr, dispatch);
		return;
	}

	// multi-statements
	dispatch_mgr_send(&dtr->dispatch_mgr, dispatch);
}

hot static inline void
executor_detach(Executor* self, Batch* batch)
{
	// group completion (called from Commit)
	auto global = &self->consensus;

	// called by Commit
	spinlock_lock(&self->lock);

	// apply global commit/abort metrics
	if (batch->commit_tsn > global->commit)
		global->commit = batch->commit_tsn;
	if (batch->abort_tsn > global->abort)
		global->abort  = batch->abort_tsn;

	// remove transactions from the executor list
	list_foreach_safe(&batch->commit)
	{
		auto dtr = list_at(Dtr, link_batch);
		list_unlink(&dtr->link);
	}

	list_foreach_safe(&batch->abort)
	{
		auto dtr = list_at(Dtr, link_batch);
		list_unlink(&dtr->link);
	}

	// wakeup access waiters
	list_foreach(&self->list_wait)
	{
		auto dtr = list_at(Dtr, link_access);
		event_signal(&dtr->on_access);
	}

	spinlock_unlock(&self->lock);
}
