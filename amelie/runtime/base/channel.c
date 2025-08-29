
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
channel_init(Channel* self, int size)
{
	ring_init(&self->ring);
	ring_prepare(&self->ring, size);
	event_init(&self->on_write);
}

void
channel_free(Channel* self)
{
	assert(! self->on_write.bus);
	ring_free(&self->ring);
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
	ring_write(&self->ring, msg);
	bus_signal(&self->on_write);
}

Msg*
channel_read(Channel* self, int time_ms)
{
	assert(self->on_write.bus);

	// non-blocking
	if (time_ms == 0)
		return ring_read(&self->ring);

	// wait indefinately
	Msg* msg;
	if (time_ms == -1)
	{
		while ((msg = ring_read(&self->ring)) == NULL)
			event_wait(&self->on_write, -1);
		return msg;
	}

	// timedwait
	msg = ring_read(&self->ring);
	if (msg)
		return msg;
	event_wait(&self->on_write, time_ms);
	return ring_read(&self->ring);
}
