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

typedef struct Ctr Ctr;
typedef struct Dtr Dtr;

typedef enum
{
	CTR_NONE,
	CTR_ACTIVE,
	CTR_COMPLETE
} CtrState;

struct Ctr
{
	Msg      msg;
	CtrState state;
	Dtr*     dtr;
	Tr*      tr;
	Core*    core;
	Ring     queue;
	bool     queue_close;
	Event    complete;
};

static inline void
ctr_init(Ctr* self, Dtr* dtr, Core* core)
{
	self->state       = CTR_NONE;
	self->dtr         = dtr;
	self->tr          = NULL;
	self->core        = core;
	self->queue_close = false;
	msg_init(&self->msg, MSG_CTR);
	ring_init(&self->queue);
	ring_prepare(&self->queue, 1024);
	event_init(&self->complete);
}

static inline void
ctr_free(Ctr* self)
{
	ring_free(&self->queue);
}

static inline void
ctr_reset(Ctr* self)
{
	self->state       = CTR_NONE;
	self->tr          = NULL;
	self->queue_close = false;
}

static inline void
ctr_attach(Ctr* self)
{
	event_attach(&self->complete);
}

static inline void
ctr_complete(Ctr* self)
{
	event_signal(&self->complete);
}
