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

typedef struct DtrQueue DtrQueue;

struct DtrQueue
{
	uint64_t id_pending;
	Dtr*     queue;
};

static inline void
dtr_queue_init(DtrQueue* self)
{
	self->id_pending = 1;
	self->queue      = NULL;
}

static inline void
dtr_queue_add(DtrQueue* self, Dtr* dtr)
{
	Dtr* prev = NULL;
	auto pos = self->queue;
	while (pos)
	{
		if (dtr->id < pos->id)
			break;
		prev = pos;
		pos = pos->link_queue;
	}
	if (prev)
	{
		dtr->link_queue = prev->link_queue;
		prev->link_queue = dtr;
	} else
	{
		dtr->link_queue = self->queue;
		self->queue = dtr;
	}
}

static inline Dtr*
dtr_queue_peek(DtrQueue* self)
{
	if (self->queue && self->queue->id == self->id_pending)
		return self->queue;
	return NULL;
}

static inline void
dtr_queue_pop(DtrQueue* self)
{
	assert(self->queue);
	auto next = self->queue;
	self->queue = self->queue->link_queue;
	self->id_pending++;
	next->link_queue = NULL;
}

static inline Dtr*
dtr_queue_next(DtrQueue* self)
{
	auto dtr = dtr_queue_peek(self);
	if (! dtr)
		return NULL;
	dtr_queue_pop(self);
	return dtr;
}
