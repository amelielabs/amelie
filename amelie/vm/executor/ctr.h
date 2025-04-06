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
	CtrState state;
	Dtr*     dtr;
	Tr*      tr;
	Core*    core;
	ReqQueue queue;
	List     link;
};

static inline void
ctr_init(Ctr* self, Dtr* dtr, Core* core)
{
	self->state = CTR_NONE;
	self->dtr   = dtr;
	self->tr    = NULL;
	self->core  = core;
	req_queue_init(&self->queue);
	list_init(&self->link);
}

static inline void
ctr_free(Ctr* self)
{
	req_queue_free(&self->queue);
}

static inline void
ctr_reset(Ctr* self)
{
	self->state = CTR_NONE;
	self->dtr   = NULL;
	self->tr    = NULL;
	req_queue_reset(&self->queue);
	list_init(&self->link);
}

static inline void
ctr_begin(Ctr* self)
{
	assert(self->state == CTR_NONE);
	self->state = CTR_ACTIVE;
}

static inline void
ctr_complete(Ctr* self)
{
	assert(self->state == CTR_ACTIVE);
	self->state = CTR_COMPLETE;
}
