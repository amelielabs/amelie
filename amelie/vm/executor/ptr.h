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

typedef struct Ptr Ptr;
typedef struct Dtr Dtr;

struct Ptr
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

static inline Ptr*
ptr_allocate(Dtr* dtr, Complete* complete)
{
	auto self = (Ptr*)am_malloc(sizeof(Ptr));
	self->dtr      = dtr;
	self->tr       = NULL;
	self->error    = NULL;
	self->closed   = false;
	self->part     = NULL;
	self->complete = complete;
	mailbox_init(&self->queue);
	msg_init(&self->queue_close, MSG_PTR_STOP);
	msg_init(&self->msg, MSG_PTR);
	list_init(&self->link);
	return self;
}

static inline void
ptr_free(Ptr* self)
{
	am_free(self);
}

static inline void
ptr_reset(Ptr* self)
{
	self->tr     = NULL;
	self->error  = NULL;
	self->closed = false;
}

static inline void
ptr_complete(Ptr* self)
{
	complete_signal(self->complete);
}

static inline void
ptr_send(Ptr* self, Msg* msg)
{
	mailbox_append(&self->queue, msg);
	event_signal(&self->queue.event);
}
