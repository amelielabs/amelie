#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct RequestCache RequestCache;
typedef struct Request      Request;

struct Request
{
	bool          complete;
	bool          ro;
	bool          ok;
	Transaction   trx;
	Code          code;
	CodeData*     code_data;
	Command*      cmd;
	Channel*      dst;
	Channel       src;
	Buf*          error;
	RequestCache* cache;
	List          link;
};

always_inline hot static inline Request*
request_of(Buf* buf)
{
	return *(Request**)msg_of(buf)->data;
}

static inline void
request_init(Request* self, RequestCache* cache)
{
	self->complete  = false;
	self->ro        = false;
	self->ok        = false;
	self->code_data = NULL;
	self->cmd       = NULL;
	self->dst       = NULL;
	self->error     = NULL;
	self->cache     = cache;
	code_init(&self->code);
	transaction_init(&self->trx);
	channel_init(&self->src);
	list_init(&self->link);
}

static inline void
request_free(Request* self)
{
	code_free(&self->code);
	transaction_free(&self->trx);
	if (self->error)
		buf_free(self->error);
	channel_detach(&self->src);
	channel_free(&self->src, mn_task->buf_cache);
}

static inline void
request_reset(Request* self)
{
	if (self->error)
	{
		buf_free(self->error);
		self->error = NULL;
	}
	self->complete  = false;
	self->ro        = false;
	self->ok        = false;
	self->code_data = NULL;
	self->cmd       = NULL;
	self->dst       = NULL;
	event_set_parent(&self->src.on_write.event, NULL);
	code_reset(&self->code);
	list_init(&self->link);
}
