#pragma once

//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// Copyright (c) 2024 Amelie Labs.
//
// AGPL-3.0 Licensed.
//

typedef struct ReqCache ReqCache;

struct ReqCache
{
	Spinlock lock;
	ReqList  list;
};

static inline void
req_cache_init(ReqCache* self)
{
	spinlock_init(&self->lock);
	req_list_init(&self->list);
}

static inline void
req_cache_free(ReqCache* self)
{
	list_foreach_safe(&self->list.list)
	{
		auto req = list_at(Req, link);
		req_free_memory(req);
	}
	req_list_init(&self->list);
	spinlock_free(&self->lock);
}

static inline Req*
req_cache_pop(ReqCache* self)
{
	spinlock_lock(&self->lock);
	auto req = req_list_pop(&self->list);
	spinlock_unlock(&self->lock);
	return req;
}

static inline void
req_cache_push(ReqCache* self, Req* req)
{
	req_reset(req);
	spinlock_lock(&self->lock);
	req_list_add(&self->list, req);
	spinlock_unlock(&self->lock);
}

hot static inline Req*
req_cache_create(ReqCache* self)
{
	auto req = req_cache_pop(self);
	if (req) {
		list_init(&req->link);
	} else
	{
		req = req_allocate();
		req->cache = self;
	}
	return req;
}
