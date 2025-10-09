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
	Msg       msg;
	CtrState  state;
	Dtr*      dtr;
	Tr*       tr;
	Buf*      error;
	Core*     core;
	Ring      queue;
	bool      queue_close;
	Consensus consensus;
	Complete* complete;
};

static inline void
ctr_init(Ctr* self, Dtr* dtr, Core* core, Complete* complete)
{
	self->state       = CTR_NONE;
	self->dtr         = dtr;
	self->tr          = NULL;
	self->error       = NULL;
	self->core        = core;
	self->queue_close = false;
	self->complete    = complete;
	msg_init(&self->msg, MSG_CTR);
	ring_init(&self->queue);
	ring_prepare(&self->queue, 1024);
	consensus_init(&self->consensus);
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
	self->error       = NULL;
	self->queue_close = false;
}

static inline void
ctr_complete(Ctr* self)
{
	complete_signal(self->complete);
}

static inline void
ctr_set_consensus(Ctr* self, Consensus* consensus)
{
	self->consensus = *consensus;
}
