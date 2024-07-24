#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct Req Req;

struct Req
{
	int      op;
	uint8_t* arg_start;
	Buf      arg;
	Value    result;
	Route*   route;
	List     link_queue;
	List     link;
};

static inline Req*
req_allocate(void)
{
	auto self = (Req*)am_malloc(sizeof(Req));
	self->op        = 0;
	self->arg_start = NULL;
	self->route     = NULL;
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
	am_free(self);
}

static inline void
req_reset(Req* self)
{
	self->op        = 0;
	self->arg_start = NULL;
	self->route     = NULL;
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
