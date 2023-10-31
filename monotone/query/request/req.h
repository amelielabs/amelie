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
	bool        error;
	bool        ok;
	Value       stmt_value;
	int         stmt;
	Transaction trx;
	Code        code;
	CodeData*   code_data;
	Command*    cmd;
	Channel*    dst;
	Channel     src;
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
	self->error     = false;
	self->ok        = false;
	self->stmt      = -1;
	self->code_data = NULL;
	self->cmd       = NULL;
	self->dst       = NULL;
	self->cache     = cache;
	code_init(&self->code);
	value_init(&self->stmt_value);
	transaction_init(&self->trx);
	channel_init(&self->src);
	list_init(&self->link);
}

static inline void
req_free(Req* self)
{
	value_free(&self->stmt_value);
	code_free(&self->code);
	transaction_free(&self->trx);
	channel_detach(&self->src);
	channel_free(&self->src, mn_task->buf_cache);
	mn_free(self);
}

static inline void
req_reset(Req* self)
{
	self->complete  = false;
	self->error     = false;
	self->ok        = false;
	self->stmt      = -1;
	self->code_data = NULL;
	self->cmd       = NULL;
	self->dst       = NULL;
	value_free(&self->stmt_value);
	event_set_parent(&self->src.on_write.event, NULL);
	code_reset(&self->code);
	list_init(&self->link);
}
