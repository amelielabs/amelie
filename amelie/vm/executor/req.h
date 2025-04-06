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

typedef struct Req      Req;
typedef struct ReqCache ReqCache;
typedef struct Core     Core;

enum
{
	REQ_EXECUTE,
	REQ_REPLAY,
	REQ_SYNC,
	REQ_BUILD
};

struct Req
{
	int   type;
	int   start;
	Buf   arg;
	Value result;
	Buf*  error;
	Core* core;
	List  link_queue;
	List  link;
};

static inline Req*
req_allocate(void)
{
	auto self = (Req*)am_malloc(sizeof(Req));
	self->type  = 0;
	self->start = 0;
	self->error = NULL;
	self->core  = NULL;
	buf_init(&self->arg);
	value_init(&self->result);
	list_init(&self->link_queue);
	list_init(&self->link);
	return self;
}

static inline void
req_free_memory(Req* self)
{
	if (self->error)
		buf_free(self->error);
	buf_free(&self->arg);
	value_free(&self->result);
	am_free(self);
}

static inline void
req_reset(Req* self)
{
	if (self->error)
	{
		buf_free(self->error);
		self->error = NULL;
	}
	self->type  = 0;
	self->start = 0;
	self->core  = NULL;
	buf_reset(&self->arg);
	value_free(&self->result);
}
