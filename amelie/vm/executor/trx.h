#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct Trx      Trx;
typedef struct TrxCache TrxCache;

struct Trx
{
	Tr        tr;
	Code*     code;
	CodeData* code_data;
	Result*   cte;
	ReqQueue  queue;
	Route*    route;
	Local*    local;
	Channel   src;
	List      link;
	TrxCache* cache;
};

static inline Trx*
trx_allocate(TrxCache* cache)
{
	auto self = (Trx*)am_malloc(sizeof(Trx));
	self->code      = NULL;
	self->code_data = NULL;
	self->cte       = NULL;
	self->local     = NULL;
	self->route     = NULL;
	self->cache     = cache;
	tr_init(&self->tr);
	req_queue_init(&self->queue);
	channel_init(&self->src);
	list_init(&self->link);
	return self;
}

static inline void
trx_free(Trx* self)
{
	tr_free(&self->tr);
	channel_detach(&self->src);
	channel_free(&self->src);
	am_free(self);
}

static inline void
trx_reset(Trx* self)
{
	self->local     = NULL;
	self->code      = NULL;
	self->code_data = NULL;
	self->cte       = NULL;
	self->route     = NULL;
	tr_reset(&self->tr);
	req_queue_reset(&self->queue);
	// channel reset?
}

static inline void
trx_set(Trx*      self,
        Local*    local,
        Route*    route,
        Code*     code,
        CodeData* code_data,
        Result*   cte,
        Limit*    limit)
{
	self->local     = local;
	self->code      = code;
	self->code_data = code_data;
	self->route     = route;
	self->cte       = cte;
	tr_set_limit(&self->tr, limit);
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
