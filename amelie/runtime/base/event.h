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

typedef struct Event     Event;
typedef struct Bus       Bus;
typedef struct Coroutine Coroutine;

struct Event
{
	Ipc        ipc;
	bool       signal;
	Bus*       bus;
	Coroutine* wait;
	Event*     parent;
};

static inline void
event_init_bus(Event* self, Bus* bus)
{
	self->signal = false;
	self->bus    = bus;
	self->wait   = NULL;
	self->parent = NULL;
	ipc_init(&self->ipc, IPC_EVENT);
}

static inline void
event_init(Event* self)
{
	event_init_bus(self, NULL);
}

static inline void
event_set_bus(Event* self, Bus* bus)
{
	self->bus = bus;
}

static inline void
event_set_parent(Event* self, Event* parent)
{
	self->parent = parent;
}

static inline bool
event_attached(Event* self)
{
	return self->bus != NULL;
}
