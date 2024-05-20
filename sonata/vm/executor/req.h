#pragma once

//
// sonata.
//
// SQL Database for JSON.
//

typedef struct Req Req;

struct Req
{
	int   op;
	int   order;
	Buf   arg;
	Value result;
	List  link_queue;
	List  link;
};

static inline Req*
req_allocate(void)
{
	auto self = (Req*)so_malloc(sizeof(Req));
	self->op    = 0;
	self->order = 0;
	buf_init(&self->arg);
	value_init(&self->result);
	list_init(&self->link_queue);
	list_init(&self->link);
	return self;
}

static inline void
req_free(Req* self)
{
	buf_free(&self->arg);
	value_free(&self->result);
	so_free(self);
}

static inline void
req_reset(Req* self)
{
	self->op    = 0;
	self->order = 0;
	buf_reset(&self->arg);
	value_free(&self->result);
	list_init(&self->link_queue);
	list_init(&self->link);
}

always_inline static inline Req*
req_of(Buf* buf)
{
	return *(Req**)msg_of(buf)->data;
}
