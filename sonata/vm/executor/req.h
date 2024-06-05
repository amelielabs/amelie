#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct Req Req;

struct Req
{
	int    op;
	Buf    arg;
	Value  result;
	Route* route;
	List   link_queue;
	List   link;
};

static inline Req*
req_allocate(void)
{
	auto self = (Req*)so_malloc(sizeof(Req));
	self->op    = 0;
	self->route = NULL;
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
	self->route = NULL;
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
