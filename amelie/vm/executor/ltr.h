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
	Consensus consensus;
	Buf*      error;
	Part*     part;
	bool      closed;
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
	self->part     = NULL;
	self->closed   = false;
	self->complete = complete;
	mailbox_init(&self->queue);
	msg_init(&self->queue_close, MSG_LTR_STOP);
	msg_init(&self->msg, MSG_LTR);
	consensus_init(&self->consensus);
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
	self->part   = NULL;
	self->closed = false;
	list_init(&self->link);
}

static inline Msg*
ltr_read(Ltr* self)
{
	return mailbox_pop(&self->queue, am_self());
}

static inline void
ltr_write(Ltr* self, Msg* msg)
{
	mailbox_append(&self->queue, msg);
	event_signal(&self->queue.event);
}

static inline void
ltr_complete(Ltr* self)
{
	complete_signal(self->complete);
}
