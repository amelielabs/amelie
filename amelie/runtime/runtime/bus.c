
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

#include <amelie_runtime.h>

void
bus_init(Bus* self)
{
	spinlock_init(&self->lock);
	list_init(&self->list_ready);
	list_init(&self->list);
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
	spinlock_lock(&self->lock);
	if (event->bus)
	{
		assert(event->bus == self);
		spinlock_unlock(&self->lock);
		return;
	}
	event->bus = self;
	list_append(&self->list, &event->link);
	spinlock_unlock(&self->lock);
}

void
bus_detach(Event* event)
{
	if (! event->bus)
		return;
	Bus* self = event->bus;
	spinlock_lock(&self->lock);
	event->bus = NULL;
	list_unlink(&event->link);
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
	list_append(&self->list_ready, &event->link_ready);
	spinlock_unlock(&self->lock);

	notify_signal(&self->notify);
}

void
bus_step(Bus* self)
{
	spinlock_lock(&self->lock);
	list_foreach_safe(&self->list_ready)
	{
		auto event = list_at(Event, link_ready);
		event_signal_direct(event);
		list_init(&event->link_ready);
	}
	list_init(&self->list_ready);
	spinlock_unlock(&self->lock);
}
