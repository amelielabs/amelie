
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
	self->lsn           = 0;
	self->current       = NULL;
	self->shutdown      = false;
	self->waiters_count = 0;
	self->pages_count   = 0;
	self->slots_count   = 0;
	spinlock_init(&self->lock);
	list_init(&self->waiters);
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

static void
cdc_notify(Cdc* self)
{
	// wakeup waiters
	while (self->waiters_count > 0)
	{
		auto waiter = container_of(list_pop(&self->waiters), CdcWaiter, link);
		self->waiters_count--;
		event_signal(&waiter->event);
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
cdc_gc(Cdc* self)
{
	spinlock_lock(&self->lock);

	// get min across all slots
	uint64_t min = UINT64_MAX;
	list_foreach_safe(&self->slots)
	{
		auto slot = list_at(CdcSlot, link);
		auto lsn  = atomic_u64_of(&slot->lsn);
		if (lsn < min)
			min = lsn;
	}

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
cdc_add(Cdc*     self, uint64_t lsn,
        Cmd      cmd,
        Uuid*    id,
        uint8_t* data,
        uint32_t data_size)
{
	// maybe create new page
	auto left = self->current->end - self->current->pos;
	if (unlikely(left < data_size))
		cdc_page_add(self);

	// reserve
	auto page = self->current;
	auto at   = page->data + page->pos;
	page->pos += data_size;

	// write event data
	auto event = (CdcEvent*)at;
	event->lsn       = lsn;
	event->cmd       = cmd;
	event->id        = *id;
	event->data_size = data_size;
	memcpy(event->data, data, data_size);

	// set last lsn
	self->lsn = lsn;
}

hot void
cdc_write(Cdc*     self, uint64_t lsn,
          Cmd      cmd,
          Uuid*    id,
          uint8_t* data,
          uint32_t data_size)
{
	spinlock_lock(&self->lock);

	// add event to the queue
	cdc_add(self, lsn, cmd, id, data, data_size);

	// wakeup waiters
	cdc_notify(self);

	spinlock_unlock(&self->lock);
}

hot void
cdc_write_batch(Cdc* self, uint64_t lsn, List* batch)
{
	spinlock_lock(&self->lock);

	// add all records from the batch
	list_foreach(batch)
	{
		auto ref = list_at(WriteCdc, link);
		auto pos = (WriteCdcRecord*)ref->data.start;
		auto end = (WriteCdcRecord*)ref->data.end;
		for (; pos < end; pos++)
			cdc_add(self, lsn, pos->cmd, pos->id, pos->data, pos->data_size);
	}

	// wakeup waiters
	cdc_notify(self);

	spinlock_unlock(&self->lock);
}
