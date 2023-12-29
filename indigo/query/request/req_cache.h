#pragma once

//
// indigo
//
// SQL OLTP database
//

typedef struct ReqCache ReqCache;

struct ReqCache
{
	Spinlock lock;
	int      list_count;
	List     list;
};

static inline void
req_cache_init(ReqCache* self)
{
	self->list_count = 0;
	spinlock_init(&self->lock);
	list_init(&self->list);
}

static inline void
req_cache_free(ReqCache* self)
{
	list_foreach_safe(&self->list)
	{
		auto req = list_at(Req, link);
		req_free(req);
	}
	spinlock_free(&self->lock);
}

static inline Req*
req_cache_pop(ReqCache* self)
{
	spinlock_lock(&self->lock);
	Req* req = NULL;
	if (likely(self->list_count > 0))
	{
		auto first = list_pop(&self->list);
		req = container_of(first, Req, link);
		self->list_count--;
	}
	spinlock_unlock(&self->lock);
	return req;
}

static inline void
req_cache_push(ReqCache* self, Req* req)
{
	req_reset(req);
	spinlock_lock(&self->lock);
	list_append(&self->list, &req->link);
	self->list_count++;
	spinlock_unlock(&self->lock);
}

static inline Req*
req_create(ReqCache* self)
{
	auto req = req_cache_pop(self);
	if (req) {
		list_init(&req->link);
	} else
	{
		req = mn_malloc(sizeof(Req));
		req_init(req, self);
		guard(guard, req_free, req);
		channel_attach(&req->src);
		unguard(&guard);
	}
	return req;
}
