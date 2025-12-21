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

typedef struct Ltr Ltr;
typedef struct Dtr Dtr;

struct Ltr
{
	Msg       msg;
	Dtr*      dtr;
	Tr*       tr;
	Mailbox   queue;
	Msg       queue_close;
	bool      closed;
	Buf*      error;
	Part*     part;
	Complete* complete;
	List      link;
};

static inline Ltr*
ltr_allocate(Dtr* dtr, Complete* complete)
{
	auto self = (Ltr*)am_malloc(sizeof(Ltr));
	self->dtr      = dtr;
	self->tr       = NULL;
	self->error    = NULL;
	self->closed   = false;
	self->part     = NULL;
	self->complete = complete;
	mailbox_init(&self->queue);
	msg_init(&self->queue_close, MSG_LTR_STOP);
	msg_init(&self->msg, MSG_LTR);
	list_init(&self->link);
	return self;
}

static inline void
ltr_free(Ltr* self)
{
	am_free(self);
}

static inline void
ltr_reset(Ltr* self)
{
	self->tr     = NULL;
	self->error  = NULL;
	self->closed = false;
}

static inline void
ltr_add(Ltr* self, Msg* msg)
{
	mailbox_append(&self->queue, msg);
	event_signal(&self->queue.event);
}

static inline void
ltr_complete(Ltr* self)
{
	complete_signal(self->complete);
}
