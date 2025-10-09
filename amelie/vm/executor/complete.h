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
};

static inline void
complete_init(Complete* self)
{
	self->pending = 0;
	event_init(&self->event);
}

static inline void
complete_reset(Complete* self)
{
	self->pending = 0;
}

static inline void
complete_prepare(Complete* self, int count)
{
	self->pending = count;
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
	assert(! atomic_u32_of(&self->pending));
}
