
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
#include <amelie_server>
#include <amelie_db>
#include <amelie_pub.h>

static inline void
pub_page_add(Pub* self)
{
	// allocate new page
	const auto page_size = 256ul * 1024;
	auto page = (PubPage*)vfs_mmap(-1, page_size);
	if (unlikely(page == NULL))
		error_system();
	page->pos = 0;
	page->end = page_size - sizeof(PubPage);
	list_init(&page->link);

	// attach
	list_append(&self->pages, &page->link);
	self->pages_count++;

	// set as current
	self->current = page;
}

void
pub_init(Pub* self)
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

	pub_page_add(self);
}

void
pub_free(Pub* self)
{
	list_foreach_safe(&self->pages)
	{
		auto page = list_at(PubPage, link);
		vfs_munmap(page, page->end + sizeof(PubPage));
	}
	spinlock_free(&self->lock);
}

void
pub_attach(Pub* self, PubSlot* slot)
{
	spinlock_lock(&self->lock);
	list_append(&self->slots, &slot->link);
	self->slots_count++;
	spinlock_unlock(&self->lock);
}

void
pub_detach(Pub* self, PubSlot* slot)
{
	spinlock_lock(&self->lock);
	list_unlink(&slot->link);
	self->slots_count--;
	spinlock_unlock(&self->lock);
}

static void
pub_notify(Pub* self)
{
	// wakeup waiters
	while (self->waiters_count > 0)
	{
		auto waiter = container_of(list_pop(&self->waiters), PubWaiter, link);
		self->waiters_count--;
		event_signal(&waiter->event);
	}
}

void
pub_shutdown(Pub* self)
{
	spinlock_lock(&self->lock);
	self->shutdown = true;
	pub_notify(self);
	spinlock_unlock(&self->lock);
}

void
pub_gc(Pub* self)
{
	spinlock_lock(&self->lock);

	// get min across all slots
	uint64_t min = UINT64_MAX;
	list_foreach_safe(&self->slots)
	{
		auto slot = list_at(PubSlot, link);
		auto lsn  = atomic_u64_of(&slot->lsn);
		if (lsn < min)
			min = lsn;
	}

	// remove all pages < min (keep at least one)
	while (self->pages_count > 1)
	{
		auto first  = list_first(&self->pages);
		auto second = container_of(first->next, PubPage, link);

		// first event on the second page
		auto ev = (PubEvent*)second->data;
		if (ev->lsn <= min)
		{
			list_unlink(first);
			self->pages_count--;

			auto ref = container_of(first, PubPage, link);
			spinlock_unlock(&self->lock);

			vfs_munmap(ref, ref->end + sizeof(PubPage));

			spinlock_lock(&self->lock);
			continue;
		}
		break;
	}

	spinlock_unlock(&self->lock);
}

void
pub_write(Pub*     self, uint64_t lsn,
          uint8_t* data,
          uint32_t data_size)
{
	spinlock_lock(&self->lock);

	// maybe create new page
	auto left = self->current->end - self->current->pos;
	if (unlikely(left < data_size))
		pub_page_add(self);

	// reserve
	auto page = self->current;
	auto at   = page->data + page->pos;
	page->pos += data_size;

	// write event data
	auto event = (PubEvent*)at;
	event->lsn       = lsn;
	event->data_size = data_size;
	memcpy(event->data, data, data_size);

	// set last lsn
	self->lsn = lsn;

	// wakeup waiters
	pub_notify(self);
	spinlock_unlock(&self->lock);
}
