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
	REQ_BUILD
};

struct Req
{
	Msg       msg;
	int       type;
	int       start;
	Buf       arg;
	Code*     code;
	CodeData* code_data;
	Buf       refs;
	int       refs_count;
	Value     result;
	Event     result_ready;
	bool      result_pending;
	Buf*      error;
	bool      close;
	Core*     core;
	List      link_union;
	List      link;
};

static inline Req*
req_allocate(void)
{
	auto self = (Req*)am_malloc(sizeof(Req));
	self->type           = 0;
	self->start          = 0;
	self->code           = NULL;
	self->code_data      = NULL;
	self->error          = NULL;
	self->core           = NULL;
	self->refs_count     = 0;
	self->result_pending = false;
	msg_init(&self->msg, MSG_REQ);
	buf_init(&self->arg);
	buf_init(&self->refs);
	value_init(&self->result);
	event_init(&self->result_ready);
	list_init(&self->link_union);
	list_init(&self->link);
	return self;
}

static inline Value*
req_ref(Req* self, int order)
{
	assert(order < self->refs_count);
	return &((Value*)self->refs.start)[order];
}

static inline void
req_free_memory(Req* self)
{
	if (self->error)
		buf_free(self->error);
	value_free(&self->result);
	for  (auto i = 0; i < self->refs_count; i++)
		value_free(req_ref(self, i));
	buf_free(&self->refs);
	buf_free(&self->arg);
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
	for  (auto i = 0; i < self->refs_count; i++)
		value_free(req_ref(self, i));
	buf_reset(&self->refs);
	value_free(&self->result);

	self->type           = 0;
	self->start          = 0;
	self->code           = NULL;
	self->code_data      = NULL;
	self->core           = NULL;
	self->refs_count     = 0;
	self->result_pending = false;
	buf_reset(&self->arg);
}

static inline void
req_attach(Req* self)
{
	event_attach(&self->result_ready);
}

static inline void
req_result_ready(Req* self)
{
	if (self->result_pending)
		event_signal(&self->result_ready);
}

static inline void
req_copy_refs(Req* self, Value* refs, int refs_count)
{
	auto to = (Value*)buf_claim(&self->refs, sizeof(Value) * refs_count);
	for (auto i = 0; i < refs_count; i++)
	{
		value_init(&to[i]);
		value_copy(&to[i], refs + i);
	}
	self->refs_count = refs_count;
}
