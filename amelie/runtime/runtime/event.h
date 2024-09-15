#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct Event Event;

struct Event
{
	void*  wait;
	Event* parent;
	Event* parent_signal;
	bool   signal;
};

static inline void
event_init(Event* self)
{
	self->wait          = NULL;
	self->parent        = NULL;
	self->parent_signal = NULL;
	self->signal        = false;
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
