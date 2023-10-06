#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Request Request;

struct Request
{
	bool        active;
	bool        abort;
	bool        complete;
	bool        ro;
	Transaction trx;
	Code        code;
	Command*    cmd;
	Condition*  on_commit;
	Channel*    dst;
	Channel     src;
	Buf*        error;
};

always_inline hot static inline Request*
request_of(Buf* buf)
{
	return *(Request**)msg_of(buf)->data;
}

static inline void
request_init(Request* self)
{
	self->active    = false;
	self->abort     = false;
	self->complete  = false;
	self->ro        = false;
	self->cmd       = NULL;
	self->on_commit = NULL;
	self->dst       = NULL;
	self->error     = NULL;
	code_init(&self->code);
	transaction_init(&self->trx);
	channel_init(&self->src);
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
	code_reset(&self->code);
	self->active    = false;
	self->abort     = false;
	self->complete  = false;
	self->ro        = false;
	self->cmd       = NULL;
	self->on_commit = NULL;
	self->dst       = NULL;
}
