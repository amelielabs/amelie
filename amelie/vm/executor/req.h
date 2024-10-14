#pragma once

//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// AGPL-3.0 Licensed.
//

typedef struct Req Req;

struct Req
{
	int      start;
	Program* program;
	Buf*     args;
	Buf      arg;
	uint8_t* arg_start;
	Value    result;
	Result*  cte;
	bool     shutdown;
	Route*   route;
	Limit*   limit;
	Local*   local;
	List     link_queue;
	List     link;
};

static inline Req*
req_allocate(void)
{
	auto self = (Req*)am_malloc(sizeof(Req));
	self->start     = 0;
	self->program   = NULL;
	self->args      = NULL;
	self->arg_start = NULL;
	self->cte       = NULL;
	self->shutdown  = false;
	self->route     = NULL;
	self->limit     = NULL;
	self->local     = NULL;
	buf_init(&self->arg);
	value_init(&self->result);
	list_init(&self->link_queue);
	list_init(&self->link);
	return self;
}

static inline void
req_free(Req* self)
{
	buf_free(&self->arg);
	value_free(&self->result);
	am_free(self);
}

static inline void
req_reset(Req* self)
{
	self->start     = 0;
	self->program   = NULL;
	self->args      = NULL;
	self->arg_start = NULL;
	self->cte       = NULL;
	self->shutdown  = false;
	self->route     = NULL;
	self->limit     = NULL;
	self->local     = NULL;
	buf_reset(&self->arg);
	value_free(&self->result);
	list_init(&self->link_queue);
	list_init(&self->link);
}

static inline void
req_set(Req*     self,
        Local*   local,
        Program* program,
        Buf*     args,
        Result*  cte,
        Limit*   limit)
{
	self->local   = local;
	self->program = program;
	self->args    = args;
	self->cte     = cte;
	self->limit   = limit;
}

always_inline static inline Req*
req_of(Buf* buf)
{
	return *(Req**)msg_of(buf)->data;
}
