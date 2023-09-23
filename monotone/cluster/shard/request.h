#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Request Request;

struct Request
{
	bool       complete;
	bool       ro;
	Condition* on_commit;
	// code
	// transaction
	Channel*   dst;
	Channel    src;
	Buf*       error;
	List       link;
};

always_inline hot static inline Request*
request_of(Buf* buf)
{
	return *(Request**)msg_of(buf)->data;
}

static inline Request*
request_allocate(void)
{
	Request* self = mn_malloc(sizeof(*self));
	self->complete  = false;
	self->ro        = false;
	self->on_commit = NULL;
	self->dst       = NULL;
	self->error     = NULL;
	channel_init(&self->src);
	list_init(&self->link);
	return self;
}

static inline void
request_free(Request* self)
{
	if (self->error)
		buf_free(self->error);
	channel_detach(&self->src);
	channel_free(&self->src, mn_task->buf_cache);
	mn_free(self);
}

static inline void
request_reset(Request* self)
{
	if (self->error)
	{
		buf_free(self->error);
		self->error = NULL;
	}
	self->complete   = false;
	self->ro         = false;
	self->on_commit  = NULL;
	self->dst        = NULL;
	list_init(&self->link);
}
