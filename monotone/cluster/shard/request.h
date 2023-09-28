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
	bool        complete;
	bool        ro;
	Transaction trx;
	Buf*        buf;
	int         buf_count;
	// code
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
	self->complete  = false;
	self->ro        = false;
	self->on_commit = NULL;
	self->dst       = NULL;
	self->error     = NULL;
	self->buf       = NULL;
	self->buf_count = 0;
	transaction_init(&self->trx);
	channel_init(&self->src);
}

static inline void
request_free(Request* self)
{
	transaction_free(&self->trx);
	if (self->buf)
		buf_free(self->buf);
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
	if (self->buf)
		buf_reset(self->buf);
	self->active     = false;
	self->complete   = false;
	self->ro         = false;
	self->on_commit  = NULL;
	self->dst        = NULL;
	self->buf_count  = 0;
}
