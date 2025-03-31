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
typedef struct ReqArg   ReqArg;
typedef struct ReqCache ReqCache;
typedef struct Dtr      Dtr;

enum
{
	REQ_EXECUTE,
	REQ_REPLAY,
	REQ_COMMIT,
	REQ_ABORT,
	REQ_BUILD
};

struct ReqArg
{
	int        type;
	int        step;
	int        start;
	Buf        arg;
	Value      result;
	Buf*       error;
	Dtr*       dtr;
	Tr*        tr;
	Backlog*   backlog;
	BacklogReq backlog_req;
};

struct Req
{
	ReqArg    arg;
	ReqCache* cache;
	List      link;
};

static inline void
req_arg_init(ReqArg* self)
{
	self->type    = 0;
	self->step    = 0;
	self->start   = 0;
	self->error   = NULL;
	self->dtr     = NULL;
	self->tr      = NULL;
	self->backlog = NULL;
	buf_init(&self->arg);
	value_init(&self->result);
	backlog_req_init(&self->backlog_req);
}

static inline void
req_arg_free(ReqArg* self)
{
	if (self->error)
		buf_free(self->error);
	buf_free(&self->arg);
	value_free(&self->result);
}

static inline void
req_arg_reset(ReqArg* self)
{
	if (self->error)
	{
		buf_free(self->error);
		self->error = NULL;
	}
	self->type    = 0;
	self->step    = 0;
	self->start   = 0;
	self->dtr     = NULL;
	self->tr      = NULL;
	self->backlog = NULL;
	buf_reset(&self->arg);
	value_free(&self->result);
	backlog_req_init(&self->backlog_req);
}

static inline Req*
req_allocate(void)
{
	auto self = (Req*)am_malloc(sizeof(Req));
	self->cache = NULL;
	req_arg_init(&self->arg);
	list_init(&self->link);
	return self;
}

static inline void
req_free_memory(Req* self)
{
	req_arg_free(&self->arg);
	am_free(self);
}

static inline void
req_reset(Req* self)
{
	req_arg_reset(&self->arg);
}
