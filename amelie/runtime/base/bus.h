#pragma once

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

typedef struct Bus Bus;

struct Bus
{
	Ring    ring;
	Mailbox mailbox;
	Io*     io;
};

static inline void
bus_init(Bus* self, Io* io)
{
	self->io = io;
	ring_init(&self->ring);
	ring_prepare(&self->ring, 1024);
	mailbox_init(&self->mailbox);
}

static inline void
bus_free(Bus* self)
{
	ring_free(&self->ring);
}

hot static inline void
bus_send(Bus* self, Ipc* ipc)
{
	if (! ipc_ref(ipc))
		return;
	ring_write(&self->ring, ipc);
	io_wakeup(self->io);
}

hot static inline void
bus_step(Bus* self)
{
	Event* event;
	Ipc* ipc;
	while ((ipc = ring_read(&self->ring)))
	{
		if (ipc->id == IPC_EVENT) {
			event = (Event*)ipc;
		} else {
			auto msg = (Msg*)ipc;
			mailbox_append(&self->mailbox, msg);
			event = &self->mailbox.event;
		}
		event_signal_local(event);
		ipc_unref(ipc);
	}
}

static inline uint64_t
bus_pending(Bus* self)
{
	return ring_pending(&self->ring);
}
