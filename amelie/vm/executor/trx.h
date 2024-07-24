#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct Trx      Trx;
typedef struct TrxCache TrxCache;

struct Trx
{
	Transaction trx;
	Code*       code;
	CodeData*   code_data;
	Result*     cte;
	ReqQueue    queue;
	Route*      route;
	Channel     src;
	List        link;
	TrxCache*   cache;
};

static inline Trx*
trx_allocate(TrxCache* cache)
{
	auto self = (Trx*)so_malloc(sizeof(Trx));
	self->code      = NULL;
	self->code_data = NULL;
	self->cte       = NULL;
	self->route     = NULL;
	self->cache     = cache;
	transaction_init(&self->trx);
	req_queue_init(&self->queue);
	channel_init(&self->src);
	list_init(&self->link);
	return self;
}

static inline void
trx_free(Trx* self)
{
	transaction_free(&self->trx);
	channel_detach(&self->src);
	channel_free(&self->src);
	so_free(self);
}

static inline void
trx_reset(Trx* self)
{
	self->code      = NULL;
	self->code_data = NULL;
	self->cte       = NULL;
	self->route     = NULL;
	transaction_reset(&self->trx);
	req_queue_reset(&self->queue);
	// channel reset?
}

static inline void
trx_set(Trx*      self,
        Route*    route,
        Code*     code,
        CodeData* code_data,
        Result*   cte)
{
	self->code      = code;
	self->code_data = code_data;
	self->route     = route;
	self->cte       = cte;
}

static inline void
trx_send(Trx* self, Req* req, bool shutdown)
{
	req_queue_add(&self->queue, req, shutdown);
}

static inline Buf*
trx_recv(Trx* self)
{
	auto buf = channel_read(&self->src, -1);
	auto msg = msg_of(buf);
	if (msg->id == MSG_ERROR)
		return buf;
	buf_free(buf);
	return NULL;
}

always_inline static inline Trx*
trx_of(Buf* buf)
{
	return *(Trx**)msg_of(buf)->data;
}
