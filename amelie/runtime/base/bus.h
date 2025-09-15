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
	Ring       ring;
	Mailbox    mailbox;
	atomic_u32 sleep;
	Notify     notify;
};

static inline void
bus_init(Bus* self)
{
	self->sleep = 0;
	ring_init(&self->ring);
	ring_prepare(&self->ring, 1024);
	mailbox_init(&self->mailbox);
	notify_init(&self->notify);
}

static inline void
bus_free(Bus* self)
{
	ring_free(&self->ring);
}

static inline int
bus_open(Bus* self, Poller* poller)
{
	return notify_open(&self->notify, poller, NULL, NULL);
}

static inline void
bus_close(Bus* self)
{
	notify_close(&self->notify);
}

static inline void
bus_set_sleep(Bus* self, int value)
{
	atomic_u32_set(&self->sleep, value);
}

static inline void
bus_attach(Bus* self, Event* event)
{
	event_set_bus(event, self);
}

hot static inline void
bus_send(Bus* self, Ipc* ipc)
{
	if (! ipc_ref(ipc))
		return;
	ring_write(&self->ring, ipc);

	if (atomic_u32_of(&self->sleep))
		notify_signal(&self->notify);
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
