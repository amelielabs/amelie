
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

#include <amelie_runtime>
#include <amelie_row.h>
#include <amelie_transaction.h>
#include <amelie_storage.h>
#include <amelie_object.h>
#include <amelie_heap.h>
#include <amelie_index.h>
#include <amelie_part.h>
#include <amelie_catalog.h>
#include <amelie_wal.h>

void
wal_subscribe_notify(Wal* self, uint64_t lsn)
{
	list_foreach_safe(&self->subscribes)
	{
		auto sub = list_at(WalSub, link);
		if (lsn < sub->lsn)
			continue;
		list_unlink(&sub->link);
		self->subscribes_count--;
		sub->active = false;
		wal_sub_signal(sub);
	}
}

void
wal_subscribe(Wal* self, WalSub* sub)
{
	spinlock_lock(&self->lock);
	if (state_lsn() > sub->lsn)
	{
		event_signal_local(sub->event);
		spinlock_unlock(&self->lock);
		return;
	}
	list_append(&self->subscribes, &sub->link);
	self->subscribes_count++;
	sub->active = true;
	spinlock_unlock(&self->lock);
}

void
wal_unsubscribe(Wal* self, WalSub* sub)
{
	spinlock_lock(&self->lock);
	if (sub->active)
	{
		list_unlink(&sub->link);
		self->subscribes_count--;
		sub->active = false;
	}
	spinlock_unlock(&self->lock);
}
