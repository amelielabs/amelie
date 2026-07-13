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

typedef struct Gtrs Gtrs;

struct Gtrs
{
	Spinlock   lock;
	uint64_t   id;
	uint64_t   id_group;
	List       list;
	GtrRecover recover;
};

static inline void
gtrs_init(Gtrs* self)
{
	self->id = 1;
	self->id_group = 1;
	list_init(&self->list);
	spinlock_init(&self->lock);
	gtr_recover_init(&self->recover);
}

static inline void
gtrs_free(Gtrs* self)
{
	spinlock_free(&self->lock);
}

hot static inline void
gtrs_recover(Gtrs* self, Gtr* gtr)
{
	auto recover = &self->recover;
	gtr_recover_add(recover, gtr);
	for (;;)
	{
		if (gtr_recover_pending(recover) && recover->list == gtr)
		{
			// get head and advance next expected lsn id
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
gtrs_attach(Gtrs* self, Gtr* gtr, Dispatch* dispatch)
{
	spinlock_lock(&self->lock);

	// set transaction group id
	if (gtr->write.recover)
	{
		// recovery path

		// ensure transaction is serialized around lsn
		gtrs_recover(self, gtr);

		// force commit writer to order transactions by lsn
		gtr->group       = 0;
		gtr->group_order = gtr->write.recover->record->lsn;
	} else
	{
		// match overlapping transactions
		auto is_snapshot = gtr->program->snapshot;
		list_foreach_reverse(&self->list)
		{
			auto ref = list_at(Gtr, link);

			// find overlapping partitions
			bool overlaps;
			if (is_snapshot)
				overlaps = dispatches_overlaps(&ref->dispatches, &gtr->dispatches);
			else
				overlaps = dispatches_overlaps_dispatch(&ref->dispatches, dispatch);

			// derive transaction group on overlap
			if (overlaps)
			{
				gtr->group       = ref->group;
				gtr->group_order = 0;
				list_foreach(&self->list)
				{
					auto ref = list_at(Gtr, link);
					if (ref->group != gtr->group)
						continue;
					if (ref->group_order > gtr->group_order)
						gtr->group_order = ref->group_order;
				}
				gtr->group_order++;
				break;
			}
		}

		// start new transaction group or set group order
		if (! gtr->group)
		{
			gtr->group = self->id_group++;
			gtr->group_order = 0;
		}
	}

	// set transaction id
	gtr->id = self->id++;

	// register transaction
	list_append(&self->list, &gtr->link);

	// begin execution
	dispatches_send(&gtr->dispatches, dispatch);

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
gtrs_send(Gtrs* self, Gtr* gtr, Dispatch* dispatch)
{
	auto dispatches = &gtr->dispatches;
	auto first = dispatches_is_first(dispatches);

	dispatch_prepare(dispatch);

	if (first)
	{
		// handle snapshot by creating transaction for every used partition
		// to handle multi-statement access, otherwise use
		// partitions from dispatch
		int ltrs_count;
		if (gtr->program->snapshot)
		{
			dispatches_snapshot(dispatches, &gtr->program->access);
			ltrs_count = dispatches->ltrs_count;
		} else {
			ltrs_count = dispatch->list_count;
		}
		complete_prepare(&dispatches->complete, ltrs_count);

		// register transaction and begin execution
		gtrs_attach(self, gtr, dispatch);
		return;
	}

	// multi-statement request
	dispatches_send(dispatches, dispatch);
}

hot static inline uint64_t
gtrs_detach(Gtrs* self, Batch* batch)
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

	// remove transactions from the gtrs list
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

hot static inline void
gtrs_sync(Gtrs* self)
{
	// sync recover state to current lsn
	spinlock_lock(&self->lock);
	gtr_recover_reset(&self->recover);
	spinlock_unlock(&self->lock);
}
