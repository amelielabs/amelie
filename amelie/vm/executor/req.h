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

typedef struct Req Req;

enum
{
	REQ_UNDEF,
	REQ_EXECUTE,
	REQ_REPLAY,
	REQ_SHUTDOWN
};

struct Req
{
	int      start;
	Program* program;
	Buf*     arg;
	Table*   arg_table;
	Value    result;
	Result*  cte;
	Limit*   limit;
	Local*   local;
};

static inline Req*
req_of(Job* job)
{
	return (Req*)job->data.start;
}

static inline void
req_allocate(Job* job)
{
	auto self = (Req*)buf_claim(&job->data, sizeof(Req));
	self->start     = 0;
	self->program   = NULL;
	self->arg       = NULL;
	self->arg_table = NULL;
	self->cte       = NULL;
	self->limit     = NULL;
	self->local     = NULL;
	value_init(&self->result);
}

static inline void
req_free(Job* job)
{
	auto self = req_of(job);
	if (self->arg)
	{
		buf_free(self->arg);
		self->arg = NULL;
	}
	value_free(&self->result);
}

static inline void
req_set(Req*     self,
        Local*   local,
        Program* program,
        Result*  cte,
        Limit*   limit)
{
	self->local   = local;
	self->program = program;
	self->cte     = cte;
	self->limit   = limit;
}
