#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct TrxCache TrxCache;

struct TrxCache
{
	Spinlock lock;
	int      list_count;
	List     list;
};

static inline void
trx_cache_init(TrxCache* self)
{
	self->list_count = 0;
	spinlock_init(&self->lock);
	list_init(&self->list);
}

static inline void
trx_cache_free(TrxCache* self)
{
	list_foreach_safe(&self->list)
	{
		auto req = list_at(Trx, link);
		trx_free(req);
	}
	spinlock_free(&self->lock);
}

static inline Trx*
trx_cache_pop(TrxCache* self)
{
	spinlock_lock(&self->lock);
	Trx* req = NULL;
	if (likely(self->list_count > 0))
	{
		auto first = list_pop(&self->list);
		req = container_of(first, Trx, link);
		self->list_count--;
	}
	spinlock_unlock(&self->lock);
	return req;
}

static inline void
trx_cache_push(TrxCache* self, Trx* req)
{
	trx_reset(req);
	spinlock_lock(&self->lock);
	list_append(&self->list, &req->link);
	self->list_count++;
	spinlock_unlock(&self->lock);
}

static inline Trx*
trx_create(TrxCache* self)
{
	auto trx = trx_cache_pop(self);
	if (likely(trx))
	{
		list_init(&trx->link);
	} else
	{
		trx = trx_allocate(self);
		guard(trx_free, trx);
		channel_attach(&trx->src);
		unguard();
	}
	return trx;
}
