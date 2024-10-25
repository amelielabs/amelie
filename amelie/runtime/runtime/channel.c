
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
channel_init(Channel* self)
{
	buf_list_init(&self->list);
	event_init(&self->on_write);
	spinlock_init(&self->lock);
}

void
channel_free(Channel* self)
{
	assert(! self->on_write.bus);
	buf_list_free(&self->list);
	spinlock_free(&self->lock);
}

void
channel_reset(Channel* self)
{
	spinlock_lock(&self->lock);
	buf_list_free(&self->list);
	spinlock_unlock(&self->lock);
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
channel_write(Channel* self, Buf* buf)
{
	spinlock_lock(&self->lock);
	buf_list_add(&self->list, buf);
	spinlock_unlock(&self->lock);
	bus_signal(&self->on_write);
}

static inline Buf*
channel_pop(Channel* self)
{
	spinlock_lock(&self->lock);
	auto next = buf_list_pop(&self->list);
	spinlock_unlock(&self->lock);
	return next;
}

Buf*
channel_read(Channel* self, int time_ms)
{
	assert(self->on_write.bus);

	// non-blocking
	if (time_ms == 0)
		return channel_pop(self);

	// wait indefinately
	Buf* buf;
	if (time_ms == -1)
	{
		while ((buf = channel_pop(self)) == NULL)
			event_wait(&self->on_write, -1);
		return buf;
	}

	// timedwait
	buf = channel_pop(self);
	if (buf)
		return buf;
	event_wait(&self->on_write, time_ms);
	return channel_pop(self);
}
