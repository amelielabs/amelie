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
	Spinlock   lock;
	uint64_t   id;
	List       list;
	GtrRecover recover;
};

static inline void
executor_init(Executor* self)
{
	self->id = 1;
	list_init(&self->list);
	spinlock_init(&self->lock);
	gtr_recover_init(&self->recover);
}

static inline void
executor_free(Executor* self)
{
	spinlock_free(&self->lock);
}

hot static inline void
executor_recover(Executor* self, Gtr* gtr)
{
	auto recover = &self->recover;
	gtr_recover_add(recover, gtr);
	for (;;)
	{
		if (gtr_recover_pending(recover) && recover->list == gtr)
		{
			// get head and advance recover id
			gtr_recover_pop(recover);
			break;
		}

		Event event;
		event_init(&event);
		event_attach(&event);
		gtr->on_recover = &event;

		spinlock_unlock(&self->lock);

		// wait
		event_wait(&event, -1);

		spinlock_lock(&self->lock);
	}
}

hot static inline void
executor_attach(Executor* self, Gtr* gtr, Dispatch* dispatch)
{
	spinlock_lock(&self->lock);

	// recovery serialization
	//
	// ensure transaction is serialized around recover_id order
	if (gtr->write.recover)
		executor_recover(self, gtr);

	// set transaction id
	//
	uint64_t id;
	if (gtr->write.recover)
	{
		// recover id
		id = gtr->write.recover->record->tsn;
		state_tsn_follow(id);
	} else
	{
		// the id must not be modified globally on replica
		if (state_is_primary())
			id = state_tsn_next();
		else
			id = state_tsn();
	}
	gtr->id = id;

	// match overlapping transaction group
	auto is_snapshot = gtr->program->snapshot;
	list_foreach_reverse(&self->list)
	{
		auto ref = list_at(Gtr, link);

		// find overlapping partitions
		bool overlaps;
		if (is_snapshot)
			overlaps = dispatch_mgr_overlaps(&ref->dispatch_mgr, &gtr->dispatch_mgr);
		else
			overlaps = dispatch_mgr_overlaps_dispatch(&ref->dispatch_mgr, dispatch);

		// derive transaction group on overlap
		if (overlaps)
		{
			gtr->group = ref->group;
			gtr->group_order = ref->group_order + 1;
			break;
		}
	}

	// start new transaction group
	if (! gtr->group)
	{
		gtr->group = self->id++;
		gtr->group_order = 0;
	}

	// register transaction
	list_append(&self->list, &gtr->link);

	// begin execution
	dispatch_mgr_send(&gtr->dispatch_mgr, dispatch);

	// wakeup next recovery waiter
	if (gtr->write.recover && gtr_recover_pending(&self->recover))
	{
		auto head = self->recover.list;
		auto on_recover = head->on_recover;
		head->on_recover = NULL;
		event_signal(on_recover);
	}

	spinlock_unlock(&self->lock);
}

hot static inline void
executor_send(Executor* self, Gtr* gtr, Dispatch* dispatch)
{
	auto mgr = &gtr->dispatch_mgr;
	auto first = dispatch_mgr_is_first(mgr);

	dispatch_prepare(dispatch);

	if (first)
	{
		// handle snapshot by creating transaction for every used partition
		// to handle multi-statement access, otherwise use
		// partitions from dispatch
		int ltrs_count;
		if (gtr->program->snapshot)
		{
			dispatch_mgr_snapshot(mgr, &gtr->program->access);
			ltrs_count = mgr->ltrs_count;
		} else {
			ltrs_count = dispatch->list_count;
		}
		complete_prepare(&mgr->complete, ltrs_count);

		// register transaction and begin execution
		executor_attach(self, gtr, dispatch);
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
		track_lsn_follow(ref, lsn);
		ref->consensus    = ref->pending_consensus;
		ref->pending      = false;
		ref->pending_link = NULL;
		ref = next;
	}

	// remove transactions from the executor list
	for (auto it = 0; it < batch->list_count; it++)
	{
		auto gtr = batch_at(batch, it);
		list_unlink(&gtr->link);
	}

	// get min group as oldest transaction group id
	uint64_t group_min = UINT64_MAX;
	if (! list_empty(&self->list))
		group_min = container_of(list_first(&self->list), Gtr, link)->group;

	spinlock_unlock(&self->lock);
	return group_min;
}

hot static inline uint64_t
executor_recover_id(Executor* self)
{
	spinlock_lock(&self->lock);
	auto id = self->recover.id_next;
	spinlock_unlock(&self->lock);
	return id;
}
