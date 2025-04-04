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

typedef struct Event Event;

struct Event
{
	void*  wait;
	Event* parent;
	Event* parent_signal;
	bool   signal;
	void*  bus;
	bool   bus_ready;
	Event* bus_ready_next;
	List   link;
};

static inline void
event_init(Event* self)
{
	self->wait           = NULL;
	self->parent         = NULL;
	self->parent_signal  = NULL;
	self->signal         = false;
	self->bus            = NULL;
	self->bus_ready      = false;
	self->bus_ready_next = NULL;
	list_init(&self->link);
}

static inline void
event_set_parent(Event* self, Event* parent)
{
	assert(parent == NULL || !self->parent);
	self->parent = parent;
}

static inline bool
event_pending(Event* self)
{
	return self->signal;
}

static inline bool
event_attached(Event* self)
{
	return self->bus != NULL;
}
