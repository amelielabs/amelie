#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct ReqCache ReqCache;
typedef struct Req      Req;

struct Req
{
	bool        complete;
	bool        ro;
	bool        ok;
	Transaction trx;
	Code        code;
	CodeData*   code_data;
	Command*    cmd;
	Channel*    dst;
	Channel     src;
	Buf*        error;
	ReqCache*   cache;
	List        link;
};

always_inline hot static inline Req*
req_of(Buf* buf)
{
	return *(Req**)msg_of(buf)->data;
}

static inline void
req_init(Req* self, ReqCache* cache)
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
req_free(Req* self)
{
	code_free(&self->code);
	transaction_free(&self->trx);
	if (self->error)
		buf_free(self->error);
	channel_detach(&self->src);
	channel_free(&self->src, mn_task->buf_cache);
}

static inline void
req_reset(Req* self)
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
