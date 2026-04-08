
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
#include <amelie_stream.h>

static inline void
stream_page_add(Stream* self)
{
	// allocate new page
	const auto page_size = 256ul * 1024;
	auto page = (StreamPage*)vfs_mmap(-1, page_size);
	if (unlikely(page == NULL))
		error_system();
	page->pos = 0;
	page->end = page_size - sizeof(StreamPage);
	list_init(&page->link);

	// attach
	list_append(&self->pages, &page->link);
	self->pages_count++;

	// set as current
	self->current = page;
}

Stream*
stream_allocate(void)
{
	auto self = (Stream*)am_malloc(sizeof(Stream));
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

	stream_page_add(self);
	return self;
}

void
stream_free(Stream* self)
{
	list_foreach_safe(&self->pages)
	{
		auto page = list_at(StreamPage, link);
		vfs_munmap(page, page->end + sizeof(StreamPage));
	}
	spinlock_free(&self->lock);
	am_free(self);
}

void
stream_attach(Stream* self, StreamSlot* slot)
{
	spinlock_lock(&self->lock);
	list_append(&self->slots, &slot->link);
	self->slots_count++;
	spinlock_unlock(&self->lock);
}

void
stream_detach(Stream* self, StreamSlot* slot)
{
	spinlock_lock(&self->lock);
	list_unlink(&slot->link);
	self->slots_count--;
	spinlock_unlock(&self->lock);
}

static void
stream_notify(Stream* self)
{
	// wakeup waiters
	while (self->waiters_count > 0)
	{
		auto waiter = container_of(list_pop(&self->waiters), StreamWaiter, link);
		self->waiters_count--;
		event_signal(&waiter->event);
	}
}

void
stream_shutdown(Stream* self)
{
	spinlock_lock(&self->lock);
	self->shutdown = true;
	stream_notify(self);
	spinlock_unlock(&self->lock);
}

void
stream_gc(Stream* self)
{
	spinlock_lock(&self->lock);

	// get min across all slots
	uint64_t min = UINT64_MAX;
	list_foreach_safe(&self->slots)
	{
		auto slot = list_at(StreamSlot, link);
		auto lsn  = atomic_u64_of(&slot->lsn);
		if (lsn < min)
			min = lsn;
	}

	// remove all pages < min (keep at least one)
	while (self->pages_count > 1)
	{
		auto first  = list_first(&self->pages);
		auto second = container_of(first->next, StreamPage, link);

		// first event on the second page
		auto ev = (StreamEvent*)second->data;
		if (ev->lsn <= min)
		{
			list_unlink(first);
			self->pages_count--;

			auto ref = container_of(first, StreamPage, link);
			spinlock_unlock(&self->lock);

			vfs_munmap(ref, ref->end + sizeof(StreamPage));

			spinlock_lock(&self->lock);
			continue;
		}
		break;
	}

	spinlock_unlock(&self->lock);
}

void
stream_write(Stream*  self, uint64_t lsn,
             uint8_t* data,
             uint32_t data_size)
{
	spinlock_lock(&self->lock);

	// maybe create new page
	auto left = self->current->end - self->current->pos;
	if (unlikely(left < data_size))
		stream_page_add(self);

	// reserve
	auto page = self->current;
	auto at   = page->data + page->pos;
	page->pos += data_size;

	// write event data
	auto event = (StreamEvent*)at;
	event->lsn       = lsn;
	event->data_size = data_size;
	memcpy(event->data, data, data_size);

	// set last lsn
	self->lsn = lsn;

	// wakeup waiters
	stream_notify(self);
	spinlock_unlock(&self->lock);
}
