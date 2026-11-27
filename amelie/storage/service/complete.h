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

typedef struct Complete Complete;

struct Complete
{
	atomic_u32 pending;
	Event      event;
	void*      arg;
};

static inline void
complete_init(Complete* self, int count, void* arg)
{
	self->pending = count;
	self->arg     = arg;
	event_init(&self->event);
	event_attach(&self->event);
}

static inline void
complete_signal(Complete* self)
{
	if (atomic_u32_dec(&self->pending) > 1)
		return;
	event_signal(&self->event);
}

static inline void
complete_wait(Complete* self)
{
	event_wait(&self->event, -1);
}
