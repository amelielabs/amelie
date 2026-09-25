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

typedef struct StreamSub StreamSub;

struct StreamSub
{
	Event*   event;
	uint64_t id;
	bool     active;
	bool     shutdown;
	List     link;
};

static inline void
stream_sub_init(StreamSub* self, Event* event, uint64_t id)
{
	self->event    = event;
	self->id       = id;
	self->active   = false;
	self->shutdown = false;
	list_init(&self->link);
}

static inline void
stream_sub_signal(StreamSub* self)
{
	event_signal(self->event);
}
