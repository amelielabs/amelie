
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
#include <amelie_cdc.h>

static inline void
cdc_page_add(Cdc* self)
{
	// allocate new page
	const auto page_size = 256ul * 1024;
	auto page = (CdcPage*)vfs_mmap(-1, page_size);
	if (unlikely(page == NULL))
		error_system();
	page->pos = 0;
	page->end = page_size - sizeof(CdcPage);
	list_init(&page->link);

	// attach
	list_append(&self->pages, &page->link);
	self->pages_count++;

	// set as current
	self->current = page;
}

void
cdc_init(Cdc* self)
{
	self->lsn         = 0;
	self->current     = NULL;
	self->shutdown    = false;
	self->subs_count  = 0;
	self->pages_count = 0;
	self->slots_count = 0;
	spinlock_init(&self->lock);
	list_init(&self->subs);
	list_init(&self->pages);
	list_init(&self->slots);
	list_init(&self->link);

	cdc_page_add(self);
}

void
cdc_free(Cdc* self)
{
	list_foreach_safe(&self->pages)
	{
		auto page = list_at(CdcPage, link);
		vfs_munmap(page, page->end + sizeof(CdcPage));
	}
	spinlock_free(&self->lock);
}

static void
cdc_notify(Cdc* self)
{
	// wakeup subscribers
	while (self->subs_count > 0)
	{
		auto sub = container_of(list_pop(&self->subs), CdcSub, link);
		self->subs_count--;
		sub->active = false;
		cdc_sub_signal(sub);
	}
}

void
cdc_shutdown(Cdc* self)
{
	spinlock_lock(&self->lock);
	self->shutdown = true;
	cdc_notify(self);
	spinlock_unlock(&self->lock);
}

void
cdc_attach(Cdc* self, CdcSlot* slot)
{
	spinlock_lock(&self->lock);
	list_append(&self->slots, &slot->link);
	self->slots_count++;
	spinlock_unlock(&self->lock);
	slot->attached = true;
}

void
cdc_detach(Cdc* self, CdcSlot* slot)
{
	if (! slot->attached)
		return;
	spinlock_lock(&self->lock);
	list_unlink(&slot->link);
	self->slots_count--;
	spinlock_unlock(&self->lock);
	slot->attached = false;
}

void
cdc_subscribe(Cdc* self, CdcSub* sub)
{
	spinlock_lock(&self->lock);
	if (self->lsn > sub->lsn)
	{
		cdc_sub_signal(sub);
		spinlock_unlock(&self->lock);
		return;
	}
	list_append(&self->subs, &sub->link);
	self->subs_count++;
	sub->active = true;
	spinlock_unlock(&self->lock);
}

void
cdc_unsubscribe(Cdc* self, CdcSub* sub)
{
	spinlock_lock(&self->lock);
	if (sub->active)
	{
		list_unlink(&sub->link);
		self->subs_count--;
		sub->active = false;
	}
	spinlock_unlock(&self->lock);
}

static inline uint64_t
cdc_minof(Cdc* self)
{
	// get min across all slots
	uint64_t min = UINT64_MAX;
	list_foreach_safe(&self->slots)
	{
		auto slot = list_at(CdcSlot, link);
		auto lsn  = atomic_u64_of(&slot->lsn);
		if (lsn < min)
			min = lsn;
	}
	return min;
}

void
cdc_min(Cdc* self, uint64_t* minref)
{
	spinlock_lock(&self->lock);
	*minref = cdc_minof(self);
	spinlock_unlock(&self->lock);
}

void
cdc_gc(Cdc* self)
{
	spinlock_lock(&self->lock);

	// get min across all slots
	uint64_t min = cdc_minof(self);

	// remove all pages < min (keep at least one)
	while (self->pages_count > 1)
	{
		auto first  = list_first(&self->pages);
		auto second = container_of(first->next, CdcPage, link);

		// first event on the second page
		auto ev = (CdcEvent*)second->data;
		if (ev->lsn <= min)
		{
			list_unlink(first);
			self->pages_count--;

			auto ref = container_of(first, CdcPage, link);
			spinlock_unlock(&self->lock);

			vfs_munmap(ref, ref->end + sizeof(CdcPage));

			spinlock_lock(&self->lock);
			continue;
		}
		break;
	}

	spinlock_unlock(&self->lock);
}

hot static inline void
cdc_add(Cdc*     self,
        uint64_t lsn,
        uint32_t lsn_op,
        Cmd      cmd,
        Uuid*    id,
        uint8_t* data,
        uint32_t data_size)
{
	// maybe create new page
	auto size = sizeof(CdcEvent) + data_size;
	auto left = self->current->end - self->current->pos;
	if (unlikely(left < size))
		cdc_page_add(self);

	// reserve
	auto page = self->current;
	auto at   = page->data + page->pos;
	page->pos += size;

	// write event data
	auto event = (CdcEvent*)at;
	event->lsn       = lsn;
	event->lsn_op    = lsn_op;
	event->cmd       = cmd;
	event->id        = *id;
	event->data_size = data_size;
	memcpy(event->data, data, data_size);

	// set last lsn
	self->lsn = lsn;
}

hot void
cdc_write(Cdc* self, uint64_t lsn, WriteCdc* write)
{
	spinlock_lock(&self->lock);

	// add event to the queue
	uint32_t op = 0;
	auto pos = (WriteCdcRecord*)write->data.start;
	auto end = (WriteCdcRecord*)write->data.position;
	while (pos < end)
	{
		cdc_add(self, lsn, op, pos->cmd, pos->id, pos->data, pos->data_size);
		op++;
		pos = (WriteCdcRecord*)(pos->data + pos->data_size);
	}

	// wakeup subscribers
	cdc_notify(self);

	spinlock_unlock(&self->lock);
}

hot void
cdc_write_batch(Cdc* self, uint64_t lsn, List* batch)
{
	spinlock_lock(&self->lock);

	// add all records from the batch
	uint32_t op = 0;
	list_foreach(batch)
	{
		auto ref = list_at(WriteCdc, link);
		auto pos = (WriteCdcRecord*)ref->data.start;
		auto end = (WriteCdcRecord*)ref->data.position;
		while (pos < end)
		{
			cdc_add(self, lsn, op, pos->cmd, pos->id, pos->data, pos->data_size);
			op++;
			pos = (WriteCdcRecord*)(pos->data + pos->data_size);
		}
	}

	// wakeup subscribers
	cdc_notify(self);

	spinlock_unlock(&self->lock);
}
