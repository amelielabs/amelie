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
};

static inline void
executor_init(Executor* self)
{
	self->id = 1;
	list_init(&self->list);
	spinlock_init(&self->lock);
}

static inline void
executor_free(Executor* self)
{
	spinlock_free(&self->lock);
}

hot static inline void
executor_attach(Executor* self, Dtr* dtr, Dispatch* dispatch)
{
	spinlock_lock(&self->lock);

	// match overlapping transaction group
	auto is_snapshot = dtr->program->snapshot;
	list_foreach_reverse(&self->list)
	{
		auto ref = list_at(Dtr, link);
		// find overlapping partitions
		bool overlaps;
		if (is_snapshot)
			overlaps = dispatch_mgr_overlaps(&ref->dispatch_mgr, &dtr->dispatch_mgr);
		else
			overlaps = dispatch_mgr_overlaps_dispatch(&ref->dispatch_mgr, dispatch);

		// derive transaction group on overlap
		if (overlaps)
		{
			dtr->group = ref->group;
			dtr->group_order = ref->group_order + 1;
			break;
		}
	}

	// start new transaction group
	if (! dtr->group)
	{
		dtr->group = self->id++;
		dtr->group_order = 0;
	}

	// register transaction
	list_append(&self->list, &dtr->link);

	// begin execution
	dispatch_mgr_send(&dtr->dispatch_mgr, dispatch);

	spinlock_unlock(&self->lock);
}

hot static inline void
executor_send(Executor* self, Dtr* dtr, Dispatch* dispatch)
{
	auto mgr = &dtr->dispatch_mgr;
	auto first = dispatch_mgr_is_first(mgr);

	dispatch_prepare(dispatch);

	if (first)
	{
		// handle snapshot by creating transaction for every used partition
		// to handle multi-statement access, otherwise use
		// partitions from dispatch
		int ltrs_count;
		if (dtr->program->snapshot)
		{
			dispatch_mgr_snapshot(mgr, &dtr->program->access);
			ltrs_count = mgr->ltrs_count;
		} else {
			ltrs_count = dispatch->list_count;
		}
		complete_prepare(&mgr->complete, ltrs_count);

		// register transaction and begin execution
		executor_attach(self, dtr, dispatch);
		return;
	}

	// multi-statement request
	dispatch_mgr_send(mgr, dispatch);
}

hot static inline uint64_t
executor_detach(Executor* self, Batch* batch)
{
	// group completion (called from Commit)
	auto lsn = batch->write.lsn;

	// called by Commit
	spinlock_lock(&self->lock);

	// apply pending partitions metrics
	auto ref = batch->pending;
	while (ref)
	{
		auto next = ref->pending_link;
		if (track_lsn(ref) < lsn)
			track_set_lsn(ref, lsn);
		ref->consensus    = ref->pending_consensus;
		ref->pending      = false;
		ref->pending_link = NULL;
		ref = next;
	}

	// remove transactions from the executor list
	for (auto it = 0; it < batch->list_count; it++)
	{
		auto dtr = batch_at(batch, it);
		list_unlink(&dtr->link);
	}

	// get min group as oldest transaction group id
	uint64_t group_min = UINT64_MAX;
	if (! list_empty(&self->list))
		group_min = container_of(list_first(&self->list), Dtr, link)->group;

	spinlock_unlock(&self->lock);
	return group_min;
}
