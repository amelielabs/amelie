
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

#include <amelie_base.h>

void
bus_init(Bus* self)
{
	self->signals = 0;
	spinlock_init(&self->lock);
	list_init(&self->list_ready);
	notify_init(&self->notify);
}

void
bus_free(Bus* self)
{
	spinlock_free(&self->lock);
}

static inline void
bus_on_notify(void* arg)
{
	unused(arg);
	// used only to wakeup event loop
}

int
bus_open(Bus* self, Poller* poller)
{
	return notify_open(&self->notify, poller, bus_on_notify, self);
}

void
bus_close(Bus* self)
{
	notify_close(&self->notify);
}

void
bus_attach(Bus* self, Event* event)
{
	if (event->bus)
	{
		assert(event->bus == self);
		return;
	}
	event->bus = self;
}

void
bus_detach(Event* event)
{
	if (! event->bus)
		return;
	Bus* self = event->bus;
	spinlock_lock(&self->lock);
	event->bus = NULL;
	list_unlink(&event->link_ready);
	spinlock_unlock(&self->lock);
}

void
bus_signal(Event* event)
{
	Bus* self = event->bus;
	spinlock_lock(&self->lock);
	if (! list_empty(&event->link_ready))
	{
		spinlock_unlock(&self->lock);
		return;
	}
	atomic_u64_inc(&self->signals);
	list_append(&self->list_ready, &event->link_ready);
	spinlock_unlock(&self->lock);

	notify_signal(&self->notify);
}

uint64_t
bus_step(Bus* self)
{
	spinlock_lock(&self->lock);
	list_foreach_safe(&self->list_ready)
	{
		auto event = list_at(Event, link_ready);
		event_signal_local(event);
		list_init(&event->link_ready);
	}
	list_init(&self->list_ready);
	auto signals = atomic_u64_of(&self->signals);
	spinlock_unlock(&self->lock);
	return signals;
}
