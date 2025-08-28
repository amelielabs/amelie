
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
channel_init(Channel* self)
{
	list_init(&self->list);
	event_init(&self->on_write);
	spinlock_init(&self->lock);
}

void
channel_free(Channel* self)
{
	assert(! self->on_write.bus);
	spinlock_free(&self->lock);
}

void
channel_attach_to(Channel* self, Bus* bus)
{
	bus_attach(bus, &self->on_write);
}

void
channel_attach(Channel* self)
{
	bus_attach(&am_task->bus, &self->on_write);
}

void
channel_detach(Channel* self)
{
	bus_detach(&self->on_write);
}

hot void
channel_write(Channel* self, Msg* msg)
{
	spinlock_lock(&self->lock);
	list_append(&self->list, &msg->link);
	spinlock_unlock(&self->lock);
	bus_signal(&self->on_write);
}

static inline Msg*
channel_pop(Channel* self)
{
	spinlock_lock(&self->lock);
	Msg* msg = NULL;
	if (! list_empty(&self->list))
	{
		auto next = list_pop(&self->list);
		msg = container_of(next, Msg, link);
	}
	spinlock_unlock(&self->lock);
	return msg;
}

Msg*
channel_read(Channel* self, int time_ms)
{
	assert(self->on_write.bus);

	// non-blocking
	if (time_ms == 0)
		return channel_pop(self);

	// wait indefinately
	Msg* msg;
	if (time_ms == -1)
	{
		while ((msg = channel_pop(self)) == NULL)
			event_wait(&self->on_write, -1);
		return msg;
	}

	// timedwait
	msg = channel_pop(self);
	if (msg)
		return msg;
	event_wait(&self->on_write, time_ms);
	return channel_pop(self);
}
