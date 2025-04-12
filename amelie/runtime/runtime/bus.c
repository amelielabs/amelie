
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
	self->ready = NULL;
	self->ready_tail = NULL;
	spinlock_init(&self->lock);
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
	/*assert(! event->bus_ready);*/
	event->bus = NULL;
	list_unlink(&event->link);
	spinlock_unlock(&self->lock);
}

hot void
bus_signal(Event* event)
{
	Bus* self = event->bus; // FIXME
	spinlock_lock(&self->lock);
	if (event->bus_ready)
	{
		spinlock_unlock(&self->lock);
		return;
	}
	event->bus_ready = true;
	if (self->ready_tail)
		self->ready_tail->bus_ready_next = event;
	else
		self->ready = event;
	self->ready_tail = event;
	spinlock_unlock(&self->lock);

	notify_signal(&self->notify);
}

hot void
bus_step(Bus* self)
{
	spinlock_lock(&self->lock);
	auto ready = self->ready;
	while (ready)
	{
		event_signal_direct(ready);
		auto next = ready->bus_ready_next;
		ready->bus_ready = false;
		ready->bus_ready_next = NULL;
		ready = next;
	}
	self->ready = NULL;
	self->ready_tail = NULL;
	spinlock_unlock(&self->lock);
}
