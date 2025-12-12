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
	Spinlock lock;
	uint64_t id;
	List     list;
	List     list_wait;
};

static inline void
executor_init(Executor* self)
{
	self->id = 1;
	list_init(&self->list);
	list_init(&self->list_wait);
	spinlock_init(&self->lock);
}

static inline void
executor_free(Executor* self)
{
	spinlock_free(&self->lock);
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
executor_attach(Executor* self, Dtr* dtr)
{
	spinlock_lock(&self->lock);

	// process exclusive transaction locking
	executor_attach_lock(self, dtr);

	// register transaction
	dtr->id = self->id++;
	list_append(&self->list, &dtr->link);

	// begin execution
	auto mgr = &dtr->dispatch_mgr;
	for (auto order = 0; order < mgr->ctrs_count; order++)
	{
		auto ctr = dispatch_mgr_ctr(mgr, order);
		if (ctr->state == CTR_ACTIVE)
			core_add(ctr->core, ctr);
	}

	spinlock_unlock(&self->lock);
}

hot static inline void
executor_detach(Executor* self, Prepare* prepare, bool abort)
{
	// group completion

	// called by Commit
	spinlock_lock(&self->lock);

	// schedule commit/abort for all involved cores
	auto cores_count = prepare->core_mgr->cores_count;
	auto cores       = (Core**)prepare->cores.start;
	if (unlikely(abort))
	{
		for (auto order = 0; order < cores_count; order++)
			if (cores[order])
				cores[order]->consensus.abort++;
	} else
	{
		for (auto order = 0; order < cores_count; order++)
			if (cores[order])
				cores[order]->consensus.commit = prepare->id_max;
	}

	// remove transactions from the list
	for (auto dtr = prepare->list; dtr; dtr = dtr->link_queue)
	{
		dtr->abort = abort;
		list_unlink(&dtr->link);
	}

	// handle abort
	if (unlikely(abort))
	{
		list_foreach(&self->list)
		{
			auto ref = list_at(Dtr, link);
			dtr_set_abort(ref);
		}
	}

	// wakeup access waiters
	list_foreach(&self->list_wait)
	{
		auto dtr = list_at(Dtr, link_access);
		event_signal(&dtr->on_access);
	}

	spinlock_unlock(&self->lock);
}

hot static inline void
executor_send(Executor* self, Dtr* dtr, Dispatch* dispatch)
{
	auto mgr = &dtr->dispatch_mgr;
	auto first = dispatch_mgr_is_first(mgr);

	// start local transactions and queue requests for execution
	dtr_send(dtr, dispatch);

	// register transaction and begin execution
	if (first)
		executor_attach(self, dtr);
}
