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

typedef struct Ctr      Ctr;
typedef struct Dtr      Dtr;
typedef struct Dispatch Dispatch;

struct Ctr
{
	Msg       msg;
	Dtr*      dtr;
	Tr*       tr;
	Buf*      error;
	Core*     core;
	Dispatch* last;
};

static inline void
ctr_init(Ctr* self, Dtr* dtr, Core* core)
{
	self->dtr   = dtr;
	self->tr    = NULL;
	self->error = NULL;
	self->core  = core;
	self->last  = NULL;
	msg_init(&self->msg, MSG_CTR);
}

static inline void
ctr_reset(Ctr* self)
{
	self->tr    = NULL;
	self->error = NULL;
	self->last  = NULL;
}
